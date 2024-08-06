#![feature(new_uninit, slice_split_once)]

use core::{
	cell::RefCell,
	cmp,
	ffi::CStr,
	fmt::{self, Write},
	future::Future,
	hash::{Hash, Hasher},
	iter::Fuse,
	mem::MaybeUninit,
	ops::Range,
	pin::Pin,
	sync::atomic::{AtomicUsize, Ordering},
	time::Duration,
};
use std::{
	collections::HashMap,
	ffi::CString,
	os::unix::ffi::OsStringExt,
	task::{Context, Poll},
	thread,
};

use rustix::{
	fd::{AsFd, BorrowedFd, OwnedFd},
	fs::{
		linkat,
		openat2,
		renameat_with,
		unlink,
		Advice,
		AtFlags,
		Dir,
		FileType,
		Mode,
		OFlags,
		RenameFlags,
		ResolveFlags,
		StatxFlags,
		CWD,
	},
	io::Errno,
	io_uring::{open_how, IoringSqeFlags},
};
use uring_async::{
	block_on,
	ops::{Close, Fadvise, Openat2, Read, Statx},
	Semaphore,
	Uring,
};

#[derive(Debug, Clone)]
struct FileInfo {
	ino: u64,
	hash: u64,
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
";

fn parse_args() -> Result<Args, lexopt::Error> {
	use lexopt::prelude::*;

	let mut args = Args {
		minsize: 1,
		maxsize: u64::MAX,
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
				args.minsize = parser.value()?.parse()?;
			}
			Short('S') | Long("maxsize") => {
				args.maxsize = parser.value()?.parse()?;
			}
			Short('a') | Long("noatime") => {
				args.noatime = true;
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
	dir_oflags: OFlags,
	dir_open_how: open_how,
	file_open_how: open_how,
}

const MAX_FILES: usize = 1024;

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
			Ok(dir) => recurse_dir(&globals, dir, path, &mut map),
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
	eprintln!("{} inodes left", inodes_small_count);

	let groups_small_read: RefCell<Vec<FileGroup>> = Default::default();
	let groups_final: RefCell<Vec<FileGroup>> = Default::default();

	let fd_semaphore = Semaphore::new(MAX_FILES);

	const SMALL_READ_SIZE: u64 = 16 * 1024;

	fn progress(total_count: usize, cur_count: &AtomicUsize) {
		eprintln!();
		loop {
			let cur_load = cur_count.load(Ordering::Relaxed);
			if cur_load == usize::MAX {
				eprintln!();
				break;
			}
			eprint!(
				"\r{}/{} ({:.2}%)",
				cur_load,
				total_count,
				100.0 * (cur_load as f32) / (total_count as f32)
			);
			thread::sleep(Duration::from_millis(500));
		}
	}

	let completion_count = AtomicUsize::new(0);
	thread::scope(|s| {
		let _ = thread::Builder::new()
			.name("progress".into())
			.spawn_scoped(s, || progress(inodes_small_count, &completion_count));

		block_on(
			&globals.ring,
			IteratorJoin::<_, _>::new(
				MAX_FILES,
				groups.into_iter().map(|group| {
					hash_group(
						&globals,
						group,
						&groups_small_read,
						&fd_semaphore,
						&completion_count,
						0..SMALL_READ_SIZE,
					)
				}),
			),
		);

		completion_count.store(usize::MAX, Ordering::Relaxed);
	});

	let inodes_final_count = groups_small_read
		.borrow()
		.iter()
		.flat_map(|x| x.files.iter())
		.count();

	eprintln!("{} inodes left (small read)", inodes_final_count,);

	let completion_count = AtomicUsize::new(0);
	thread::scope(|s| {
		let _ = thread::Builder::new()
			.name("progress".into())
			.spawn_scoped(s, || progress(inodes_final_count, &completion_count));

		block_on(
			&globals.ring,
			IteratorJoin::<_, _>::new(
				MAX_FILES,
				groups_small_read.into_inner().into_iter().map(|group| {
					hash_group(
						&globals,
						group,
						&groups_final,
						&fd_semaphore,
						&completion_count,
						(SMALL_READ_SIZE + 1)..u64::MAX,
					)
				}),
			),
		);

		completion_count.store(usize::MAX, Ordering::Relaxed);
	});

	const TMP_FILE_NAME: &[u8] = b"DUPELINK_TMP_FILE";

	let mut total_deduped_files: u64 = 0;
	let mut total_deduped_size: u64 = 0;

	let mut buffer = String::new();
	let mut path_buf = Vec::new();
	for group in groups_final.into_inner() {
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

fn recurse_dir(
	globals: &Globals,
	dirfd: OwnedFd,
	dir_path: &CStr,
	map: &mut HashMap<GroupInfo, Vec<FileInfo>>,
) {
	let dir_iter = Dir::read_from(dirfd.as_fd()).unwrap();

	let mut files = Vec::new();
	let mut dirs = Vec::new();

	for dentry in dir_iter.skip(2) {
		let dentry = dentry.unwrap();

		match dentry.file_type() {
			FileType::RegularFile => {
				let file_name = dentry.file_name().to_owned();
				let dirfd_borrow: BorrowedFd<'_> = dirfd.as_fd();
				files.push(FutureOrOutput::Future(async move {
					let mut statx_buf = MaybeUninit::uninit();
					let statx = Statx::new(
						&globals.ring,
						dirfd_borrow,
						&file_name,
						AtFlags::empty(),
						StatxFlags::INO
							| StatxFlags::TYPE | StatxFlags::MODE
							| StatxFlags::UID | StatxFlags::GID
							| StatxFlags::MNT_ID,
						&mut statx_buf,
						IoringSqeFlags::empty(),
					)
					.await
					.map(|statx| *statx);

					(statx, file_name)
				}));
			}
			FileType::Directory => {
				dirs.push(dentry.file_name().to_owned());
			}
			_ => (),
		}
	}

	dirs.shrink_to_fit();

	block_on(&globals.ring, SliceJoin(&mut files));

	let mut path_buf = Vec::new();
	for output in files {
		let (statx, file_path) = output.unwrap_output();
		let statx = match statx {
			Ok(statx) => statx,
			Err(err) => {
				eprintln!("Error statting {:?}: {}", file_path, err);
				continue;
			}
		};

		if statx.stx_size < globals.minsize || statx.stx_size > globals.maxsize {
			continue;
		}

		let mut hasher = std::hash::DefaultHasher::new();
		hasher.write_u16(statx.stx_mode);
		hasher.write_u32(statx.stx_uid);
		hasher.write_u32(statx.stx_gid);
		hasher.write_u64(statx.stx_mnt_id);

		let group_info = GroupInfo {
			size: statx.stx_size,
			hashed: hasher.finish(),
		};

		path_concat(&mut path_buf, dir_path, &file_path);
		let full_path = CString::from_vec_with_nul(path_buf.clone()).unwrap();
		let file_info = FileInfo {
			ino: statx.stx_ino,
			path: MegaName::from_cstring(full_path),
			hash: 0,
		};

		map.entry(group_info).or_default().push(file_info);
	}

	let mut path_buf = Vec::new();

	for new_dir_path in dirs {
		let path = path_concat(&mut path_buf, dir_path, &new_dir_path);

		match openat2(
			dirfd.as_fd(),
			&new_dir_path,
			globals.dir_oflags,
			Mode::empty(),
			globals.dir_open_how.resolve,
		) {
			Ok(new_dirfd) => recurse_dir(globals, new_dirfd, path, map),
			Err(Errno::XDEV) => (),
			Err(err) => {
				eprintln!("Error opening {:?}: {}", path, err);
				continue;
			}
		}
	}
}

async fn hash_group(
	globals: &Globals,
	mut group: FileGroup,
	new_groups: &RefCell<Vec<FileGroup>>,
	fd_semaphore: &Semaphore,
	completion_count: &AtomicUsize,
	range: Range<u64>,
) {
	if group.info.size < range.start {
		new_groups.borrow_mut().push(group);
		return;
	}

	let mut files: Vec<_> = group
		.files
		.iter_mut()
		.map(|file| {
			FutureOrOutput::Future(async move {
				let _guard = fd_semaphore.wait().await;
				let target_size = cmp::min(group.info.size, range.end);

				completion_count.fetch_add(1, Ordering::Relaxed);

				let fd = match Openat2::new(
					&globals.ring,
					CWD,
					file.path.first_name(),
					&globals.file_open_how,
					IoringSqeFlags::empty(),
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

				let _ = Fadvise::new(
					&globals.ring,
					fd.as_fd(),
					range.start,
					(range.end - range.start).try_into().unwrap_or(u32::MAX),
					Advice::WillNeed,
					IoringSqeFlags::empty(),
				)
				.await;

				let mut buffer = Box::new_uninit_slice(cmp::min(
					target_size.try_into().unwrap(),
					2 * 1024 * 1024,
				));

				let mut hash = std::hash::DefaultHasher::new();
				let mut total_read = 0;
				loop {
					let to_read =
						cmp::min((target_size - total_read).try_into().unwrap(), buffer.len());
					match Read::new(
						&globals.ring,
						fd.as_fd(),
						&mut buffer[0..to_read],
						IoringSqeFlags::empty(),
					)
					.await
					{
						Ok([]) => {
							assert_eq!(total_read, target_size);
							break;
						}
						Ok(x) => {
							total_read += <usize as TryInto<u64>>::try_into(x.len()).unwrap();
							assert!(total_read <= target_size);
							hash.write(x);
						}
						Err(Errno::AGAIN | Errno::INTR | Errno::CANCELED) => (),
						Err(err) => {
							file.ino = 0;
							eprintln!("Error reading {:?}: {}", file.path.first_name(), err);
							return;
						}
					}
				}

				let _ = Fadvise::new(
					&globals.ring,
					fd.as_fd(),
					range.start,
					(range.end - range.start).try_into().unwrap_or(u32::MAX),
					Advice::DontNeed,
					IoringSqeFlags::empty(),
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
