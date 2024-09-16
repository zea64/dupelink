#![feature(new_uninit, slice_split_once, noop_waker)]

mod scan;

use core::{
	cell::{Cell, RefCell},
	cmp,
	ffi::CStr,
	fmt::{self, Write},
	future::Future,
	hash::{Hash, Hasher},
	iter::Fuse,
	num::ParseIntError,
	ops::Range,
	pin::Pin,
	task::Waker,
};
use std::{
	collections::HashMap,
	ffi::CString,
	os::unix::ffi::OsStringExt,
	task::{Context, Poll},
	time::SystemTime,
};

use rustix::{
	fd::AsFd,
	fs::{
		linkat,
		openat2,
		renameat_with,
		unlink,
		Advice,
		AtFlags,
		Mode,
		OFlags,
		RenameFlags,
		ResolveFlags,
		CWD,
	},
	io::Errno,
	io_uring::open_how,
};
use uring_async::{
	ops::{Close, Fadvise, Openat2, Read, UringOp},
	sync::Semaphore,
	Uring,
};

#[derive(Debug, Clone)]
struct FileInfo {
	ino: u64,
	hash: u64,
	ctime: SystemTime,
	path: MegaName,
}

impl PartialEq for FileInfo {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other).is_eq()
	}
}

impl Eq for FileInfo {}

impl PartialOrd for FileInfo {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for FileInfo {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		let self_num = ((self.hash as u128) << 64) + self.ino as u128;
		let other_num = ((other.hash as u128) << 64) + other.ino as u128;
		self_num.cmp(&other_num)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct GroupInfo {
	size: u64,
	hashed: u64,
}

#[derive(Debug)]
struct FileGroup {
	info: GroupInfo,
	files: Vec<FileInfo>,
}

#[derive(Debug)]
struct Args {
	minsize: u64,
	maxsize: u64,
	fds: usize,
	noatime: bool,
	link: bool,
	paths: Vec<CString>,
}

const HELP_STR: &str = "\
USAGE: dupelink [OPTIONS] <PATHS>...

ARGS:
	<PATHS...>             Paths to search for duplicates

OPTIONS:
	-l, --link             Hardlink duplicate files (otherwise only print them)
	-s, --minsize <SIZE>   [default: 1] Minimum size of files to check
	-S, --maxsize <SIZE>   [default: u64::MAX] Maximum size of files to check
	-f, --fds <COUNT>      [default: 128] Maximum simultaneously open file descriptors for hashing
";

fn parse_args() -> Result<Args, lexopt::Error> {
	use lexopt::prelude::*;

	let mut args = Args {
		minsize: 1,
		maxsize: u64::MAX,
		fds: 128,
		noatime: false,
		link: false,
		paths: Vec::new(),
	};

	let mut parser = lexopt::Parser::from_env();
	while let Some(arg) = parser.next()? {
		match arg {
			Short('l') | Long("link") => {
				args.link = true;
			}
			Short('s') | Long("minsize") => {
				args.minsize = parser.value()?.parse_with(parse_size)?;
			}
			Short('S') | Long("maxsize") => {
				args.maxsize = parser.value()?.parse_with(parse_size)?;
			}
			Short('a') | Long("noatime") => {
				args.noatime = true;
			}
			Short('f') | Long("fds") => {
				args.fds = parser.value()?.parse()?;
			}
			Value(path) => {
				// Args can't have NULs, this unwrap is infallible.
				args.paths.push(CString::new(path.into_vec()).unwrap());
			}
			_ => return Err(arg.unexpected()),
		}
	}

	if args.paths.is_empty() {
		return Err("expected PATHS argument(s)".into());
	}

	Ok(args)
}

#[derive(Debug)]
struct Globals {
	ring: RefCell<Uring>,
	minsize: u64,
	maxsize: u64,
	fds: usize,
	dir_oflags: OFlags,
	dir_open_how: open_how,
	file_open_how: open_how,
}

fn main() {
	let ring = RefCell::new(Uring::new().unwrap());
	let mut map = HashMap::new();

	let args = match parse_args() {
		Ok(x) => x,
		Err(err) => {
			eprint!("{}\n{}", err, HELP_STR);
			return;
		}
	};

	let file_oflags = OFlags::RDONLY
		| OFlags::NOCTTY
		| OFlags::CLOEXEC
		| OFlags::NOFOLLOW
		| if args.noatime {
			OFlags::NOATIME
		} else {
			OFlags::empty()
		};
	let dir_oflags = file_oflags | OFlags::DIRECTORY;
	let globals = Globals {
		ring,
		minsize: args.minsize,
		maxsize: args.maxsize,
		fds: args.fds,
		dir_oflags,
		dir_open_how: open_how {
			flags: dir_oflags.bits().into(),
			mode: 0,
			resolve: ResolveFlags::NO_XDEV | ResolveFlags::NO_MAGICLINKS | ResolveFlags::BENEATH,
		},
		file_open_how: open_how {
			flags: file_oflags.bits().into(),
			mode: 0,
			resolve: ResolveFlags::NO_MAGICLINKS,
		},
	};

	for path in args.paths.iter() {
		match openat2(
			CWD,
			path,
			globals.dir_oflags,
			Mode::empty(),
			ResolveFlags::empty(),
		) {
			Ok(dir) => scan::recurse_dir(&globals, dir, path, &mut map),
			Err(err) => eprintln!("Error opening {:?}: {}", path, err),
		}
	}

	eprintln!(
		"{} inodes scanned",
		map.iter().flat_map(|(_k, v)| v.iter()).count()
	);

	let groups: Vec<FileGroup> = map
		.into_iter()
		.filter_map(|(k, mut v)| {
			if v.chunk_by(FileInfo::eq).count() == 1 {
				None
			} else {
				// Sort so same inode files are together.
				v.sort_unstable();

				// Compress multiple names into a single `MegaName`.
				let new_files: Vec<FileInfo> = v
					.chunk_by_mut(FileInfo::eq)
					.map(|infos| FileInfo {
						ino: infos[0].ino,
						hash: infos[0].hash,
						ctime: infos[0].ctime,
						path: infos.iter().map(|x| x.path.first_name()).into(),
					})
					.collect();

				Some(FileGroup {
					info: k,
					files: new_files,
				})
			}
		})
		.collect();

	let inodes_small_count = groups.iter().flat_map(|x| x.files.iter()).count();
	eprintln!("{} inodes left\n", inodes_small_count);

	let groups_small_read: RefCell<Vec<FileGroup>> = Default::default();
	let groups_final: RefCell<Vec<FileGroup>> = Default::default();

	let fd_semaphore = Semaphore::new(globals.fds);

	const SMALL_READ_SIZE: u64 = 16 * 1024;

	let completion_count = Cell::new(0usize);
	block_on(
		&globals.ring,
		IteratorJoin::<_, _>::new(
			globals.fds,
			groups.into_iter().map(|group| {
				hash_group(
					&globals,
					group,
					&groups_small_read,
					&fd_semaphore,
					&completion_count,
					inodes_small_count,
					0..SMALL_READ_SIZE,
				)
			}),
		),
	);

	let inodes_final_count = groups_small_read
		.borrow()
		.iter()
		.flat_map(|x| x.files.iter())
		.count();

	eprintln!("\n{} inodes left (small read)\n", inodes_final_count,);

	let completion_count = Cell::new(0usize);
	block_on(
		&globals.ring,
		IteratorJoin::<_, _>::new(
			globals.fds,
			groups_small_read.into_inner().into_iter().map(|group| {
				hash_group(
					&globals,
					group,
					&groups_final,
					&fd_semaphore,
					&completion_count,
					inodes_final_count,
					(SMALL_READ_SIZE + 1)..u64::MAX,
				)
			}),
		),
	);

	const TMP_FILE_NAME: &[u8] = b"DUPELINK_TMP_FILE";

	let mut total_deduped_files: u64 = 0;
	let mut total_deduped_size: u64 = 0;

	let mut buffer = String::new();
	let mut path_buf = Vec::new();
	for mut group in groups_final.into_inner() {
		buffer.clear();

		let nr_deduped_files: u64 = (group.files.len() - 1).try_into().unwrap();
		total_deduped_files += nr_deduped_files;
		let deduped_size = nr_deduped_files * group.info.size;
		total_deduped_size += deduped_size;

		writeln!(
			&mut buffer,
			"{} bytes each, {} bytes deduped",
			group.info.size, deduped_size,
		)
		.unwrap();

		// Sort by ctime
		group.files.sort_unstable_by(|a, b| a.ctime.cmp(&b.ctime));

		let mut names = group.files.iter().flat_map(|file| file.path.iter());
		let master_name = names.next().unwrap();

		writeln!(&mut buffer, "{:?}", master_name).unwrap();

		for name in names {
			// TODO, check that file hasn't changed.
			if args.link {
				name.to_bytes()
					.rsplit_once(|c| *c == b'/')
					.unwrap()
					.0
					.clone_into(&mut path_buf);
				path_buf.push(b'/');
				path_buf.extend_from_slice(TMP_FILE_NAME);
				path_buf.push(0);

				let tmp_name = CStr::from_bytes_with_nul(&path_buf).unwrap();

				if let Err(err) = linkat(CWD, master_name, CWD, tmp_name, AtFlags::empty()) {
					eprintln!("Error linking {:?} to {:?}: {}", master_name, tmp_name, err);
					continue;
				}

				let mut error_flag = false;
				if let Err(err) = renameat_with(CWD, name, CWD, tmp_name, RenameFlags::EXCHANGE) {
					eprintln!("Error exchanging {:?} and {:?}: {}", name, tmp_name, err);
					// Explicitly no `continue` statement, we want the unlink operation to apply unconditionally
					error_flag = true;
				}

				if let Err(err) = unlink(tmp_name) {
					eprintln!("Error unlinking {:?}: {}", tmp_name, err);
					continue;
				}

				if error_flag {
					continue;
				}
			}
			writeln!(&mut buffer, "{:?}", name).unwrap();
		}

		println!("{}", buffer);
	}

	println!(
		"\nSummary:\n{} files totalling {} bytes",
		total_deduped_files, total_deduped_size
	);
}

fn block_on<F: Future>(ring: &RefCell<Uring>, mut fut: F) -> F::Output {
	loop {
		if let Poll::Ready(x) = Future::poll(
			unsafe { Pin::new_unchecked(&mut fut) },
			&mut Context::from_waker(Waker::noop()),
		) {
			break x;
		}

		let mut borrowed_ring = ring.borrow_mut();
		let in_flight = borrowed_ring.in_flight();

		if borrowed_ring.sq_enqueued() != 0 || in_flight != 0 {
			borrowed_ring.submit(in_flight / 8 + 1);
		}
	}
}

async fn hash_group(
	globals: &Globals,
	mut group: FileGroup,
	new_groups: &RefCell<Vec<FileGroup>>,
	fd_semaphore: &Semaphore,
	completion_count: &Cell<usize>,
	total_count: usize,
	range: Range<u64>,
) {
	if group.info.size < range.start {
		new_groups.borrow_mut().push(group);
		return;
	}

	let mut files: Vec<_> =
		group
			.files
			.iter_mut()
			.map(|file| {
				FutureOrOutput::Future(async move {
					let _guard = fd_semaphore.wait().await;
					let target_size = cmp::min(group.info.size, range.end);

					let new_completion_count = completion_count.get() + 1;
					completion_count.set(new_completion_count);

					let integer_part = new_completion_count * 100 / total_count;
					let frac_part = new_completion_count * 10000 / total_count - integer_part * 100;

					eprint!("{}", format!("\r[{new_completion_count}/{total_count}] ({integer_part:.2}.{frac_part:.2}%)"));

					let fd = match Openat2::new(
						&globals.ring,
						CWD,
						file.path.first_name(),
						&globals.file_open_how,
					)
					.await
					{
						Ok(fd) => fd,
						Err(err) => {
							file.ino = 0;
							eprintln!("Error opening {:?}: {}", file.path.first_name(), err);
							return;
						}
					};

					// This will add it to the sq but *not* submit. We want to submit *with* the next read. Cleanup will happen later.
					let fadvise = Fadvise::new(
						&globals.ring,
						fd.as_fd(),
						range.start,
						(range.end - range.start).try_into().unwrap_or(u32::MAX),
						Advice::WillNeed,
					)
					.link();

					let mut buffer: Box<[u8]> =
						vec![0; cmp::min(target_size.try_into().unwrap(), 2 * 1024 * 1024,)].into();

					let mut hash = std::hash::DefaultHasher::new();
					let mut total_read: u64 = 0;
					loop {
						let to_read =
							cmp::min((target_size - total_read).try_into().unwrap(), buffer.len());
						let read =
							Read::new(&globals.ring, fd.as_fd(), u64::MAX, &mut buffer[0..to_read])
								.await;

						match read {
							Ok(0) => {
								assert_eq!(total_read, target_size);
								break;
							}
							Ok(x) => {
								total_read += <u32 as Into<u64>>::into(x);
								assert!(total_read <= target_size);
								hash.write(&buffer[0..(x.try_into().unwrap())]);
							}
							Err(Errno::AGAIN | Errno::INTR | Errno::CANCELED) => (),
							Err(err) => {
								file.ino = 0;
								eprintln!("Error reading {:?}: {}", file.path.first_name(), err);
								return;
							}
						}
					}

					// Cleanup fadvise from earlier (it doesn't have a good drop impl yet).
					let _ = fadvise.await;

					let _ = Fadvise::new(
						&globals.ring,
						fd.as_fd(),
						range.start,
						(range.end - range.start).try_into().unwrap_or(u32::MAX),
						Advice::DontNeed,
					)
					.await;

					let _ = Close::new(&globals.ring, fd).await;

					file.hash = hash.finish();
				})
			})
			.collect();

	SliceJoin(&mut files).await;
	drop(files);

	group.files.sort_unstable();

	for chunk in group.files.chunk_by(|a, b| a.hash == b.hash) {
		// Inode 0 signals an error processing that inode.
		if chunk.len() <= 1 || chunk[0].ino == 0 {
			continue;
		}
		new_groups.borrow_mut().push(FileGroup {
			info: group.info,
			files: chunk.to_owned(),
		});
	}
}

enum FutureOrOutput<F, O> {
	Future(F),
	Output(O),
}

impl<F, O> FutureOrOutput<F, O> {
	fn unwrap_output(self) -> O {
		match self {
			FutureOrOutput::Output(x) => x,
			_ => unreachable!(),
		}
	}
}

struct SliceJoin<'a, F: Future>(&'a mut [FutureOrOutput<F, <F as Future>::Output>]);

impl<'a, F: Future> Future for SliceJoin<'a, F> {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = Pin::into_inner(self);
		let mut output_count = 0;
		for fut_or_output in this.0.iter_mut() {
			match fut_or_output {
				FutureOrOutput::Output(_) => {
					output_count += 1;
				}
				FutureOrOutput::Future(f) => {
					if let Poll::Ready(output) = Future::poll(unsafe { Pin::new_unchecked(f) }, cx)
					{
						*fut_or_output = FutureOrOutput::Output(output);
						output_count += 1;
					}
				}
			}
		}

		if output_count == this.0.len() {
			Poll::Ready(())
		} else {
			Poll::Pending
		}
	}
}

struct IteratorJoin<F: Future<Output = ()>, I: Iterator<Item = F>> {
	buffer: Vec<Option<F>>,
	iter: Fuse<I>,
}

impl<F: Future<Output = ()>, I: Iterator<Item = F>> IteratorJoin<F, I> {
	fn new(n: usize, iter: I) -> Self {
		Self {
			buffer: (0..n).map(|_| None).collect(),
			iter: iter.fuse(),
		}
	}
}

impl<F: Future<Output = ()>, I: Iterator<Item = F>> Future for IteratorJoin<F, I> {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = unsafe { Pin::into_inner_unchecked(self) };

		let mut any_pending = false;
		for slot in this.buffer.iter_mut() {
			loop {
				if let Some(f) = slot {
					if Future::poll(unsafe { Pin::new_unchecked(f) }, cx).is_ready() {
						*slot = None;
					} else {
						any_pending = true;
						break;
					}
				} else if let Some(f) = this.iter.next() {
					*slot = Some(f);
				} else {
					break;
				}
			}
		}

		if any_pending {
			Poll::Pending
		} else {
			Poll::Ready(())
		}
	}
}

#[derive(Clone)]
struct MegaName(Box<[u8]>);

impl<'a> MegaName {
	fn from_cstring(cstring: CString) -> Self {
		Self(cstring.into_bytes_with_nul().into())
	}

	fn first_name(&self) -> &CStr {
		unsafe { CStr::from_ptr(self.0.as_ptr().cast()) }
	}

	fn iter(&'a self) -> MegaNameIter<'a> {
		MegaNameIter(&self.0)
	}
}

impl<'a, T: Iterator<Item = &'a CStr>> From<T> for MegaName {
	fn from(value: T) -> Self {
		Self(value.flat_map(CStr::to_bytes_with_nul).copied().collect())
	}
}

impl fmt::Debug for MegaName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_list().entries(self.iter()).finish()
	}
}

#[derive(Debug)]
struct MegaNameIter<'a>(&'a [u8]);

impl<'a> Iterator for MegaNameIter<'a> {
	type Item = &'a CStr;

	fn next(&mut self) -> Option<Self::Item> {
		if self.0.is_empty() {
			None
		} else {
			let ret = unsafe { CStr::from_ptr(self.0.as_ptr().cast()) };
			let next_index = self.0.iter().position(|x| *x == 0).unwrap() + 1;
			self.0 = &self.0[next_index..];
			Some(ret)
		}
	}
}

fn path_concat<'a, 'b>(path_buf: &'a mut Vec<u8>, str1: &'b CStr, str2: &'b CStr) -> &'a CStr {
	path_buf.clear();
	path_buf.extend_from_slice(str1.to_bytes());
	path_buf.push(b'/');
	path_buf.extend_from_slice(str2.to_bytes());
	path_buf.push(0);

	CStr::from_bytes_with_nul(path_buf).unwrap()
}

fn parse_size(mut s: &str) -> Result<u64, ParseIntError> {
	const TABLE: &[(char, u64)] = &[
		('k', 1024u64.pow(1)),
		('K', 1000u64.pow(1)),
		('m', 1024u64.pow(2)),
		('M', 1000u64.pow(2)),
		('g', 1024u64.pow(3)),
		('G', 1000u64.pow(3)),
		('t', 1024u64.pow(4)),
		('T', 1000u64.pow(4)),
	];

	let mut multiplier = 1;
	let last_char = s.chars().last().unwrap_or_default();
	if let Some((_, mult)) = TABLE.iter().find(|(c, _)| *c == last_char) {
		s = &s[0..s.len() - 1];
		multiplier = *mult;
	}

	s.parse().map(|x: u64| x * multiplier)
}
