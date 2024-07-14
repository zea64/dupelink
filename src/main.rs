#![feature(new_uninit)]

use core::{
	cell::RefCell,
	ffi::CStr,
	fmt::{self, Write},
	future::Future,
	hash::{Hash, Hasher},
	iter::Fuse,
	mem::MaybeUninit,
	pin::Pin,
};
use std::{
	cmp::Ordering,
	collections::HashMap,
	ffi::CString,
	os::unix::ffi::OsStringExt,
	task::{Context, Poll},
};

use rustix::{
	fd::{AsFd, BorrowedFd, OwnedFd},
	fs::{openat2, Advice, AtFlags, Dir, FileType, Mode, OFlags, ResolveFlags, StatxFlags, CWD},
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
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for FileInfo {
	fn cmp(&self, other: &Self) -> Ordering {
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

fn main() {
	let owned_ring = RefCell::new(Uring::new().unwrap());
	let ring = &owned_ring;
	let mut map = HashMap::new();

	let args = match parse_args() {
		Ok(x) => x,
		Err(err) => {
			eprint!("{}\n{}", err, HELP_STR);
			return;
		}
	};

	for path in args.paths.iter() {
		let dir = openat2(
			CWD,
			path,
			OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOCTTY | OFlags::CLOEXEC,
			Mode::empty(),
			ResolveFlags::empty(),
		)
		.unwrap();

		recurse_dir(ring, dir, path, &mut map);
	}

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

	let owned_new_groups: RefCell<Vec<FileGroup>> = Default::default();
	let new_groups = &owned_new_groups;

	let fd_semaphore = Semaphore::new(4096);

	block_on(
		ring,
		IteratorJoin::<4096, _, _>::new(
			groups
				.into_iter()
				.map(|group| hash_group(ring, group, new_groups, &fd_semaphore)),
		),
	);

	let mut buffer = String::new();
	for group in owned_new_groups.into_inner() {
		buffer.clear();

		writeln!(&mut buffer, "({}) ", group.info.size).unwrap();

		for file in group.files {
			for name in file.path.iter() {
				writeln!(&mut buffer, "{:?}", name).unwrap();
			}
		}

		print!("{}", buffer);
	}
}

fn recurse_dir(
	ring: &RefCell<Uring>,
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
						ring,
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

	block_on(ring, SliceJoin(&mut files));

	let mut path_buf = Vec::new();
	for output in files {
		let (statx, file_path) = match output {
			FutureOrOutput::Output((x, path)) => (x.unwrap(), path),
			_ => unreachable!(),
		};

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
		let new_dirfd = openat2(
			dirfd.as_fd(),
			&new_dir_path,
			OFlags::RDONLY
				| OFlags::DIRECTORY
				| OFlags::NOFOLLOW
				| OFlags::NOCTTY | OFlags::CLOEXEC,
			Mode::empty(),
			ResolveFlags::NO_XDEV | ResolveFlags::BENEATH,
		)
		.unwrap();
		recurse_dir(
			ring,
			new_dirfd,
			path_concat(&mut path_buf, dir_path, &new_dir_path),
			map,
		);
	}
}

async fn hash_group(
	ring: &RefCell<Uring>,
	mut group: FileGroup,
	new_groups: &RefCell<Vec<FileGroup>>,
	fd_semaphore: &Semaphore,
) {
	let mut files: Vec<_> = group
		.files
		.iter_mut()
		.map(|file| {
			FutureOrOutput::Future(async move {
				let _guard = fd_semaphore.wait().await;

				let open_how = open_how {
					flags: (OFlags::RDONLY | OFlags::NOCTTY | OFlags::CLOEXEC)
						.bits()
						.into(),
					mode: 0,
					resolve: ResolveFlags::NO_MAGICLINKS | ResolveFlags::NO_SYMLINKS,
				};

				let fd = Openat2::new(
					ring,
					CWD,
					file.path.first_name(),
					&open_how,
					IoringSqeFlags::empty(),
				)
				.await
				.unwrap_or_else(|e| panic!("{:?}: {:?}", e, file.path));

				let _ = Fadvise::new(
					ring,
					fd.as_fd(),
					0,
					0,
					Advice::WillNeed,
					IoringSqeFlags::empty(),
				)
				.await;

				let mut hash = std::hash::DefaultHasher::new();
				let mut total_read = 0;
				loop {
					let mut buffer = Box::new_uninit_slice(core::cmp::min(
						group.info.size.try_into().unwrap(),
						2 * 1024 * 1024,
					));

					match Read::new(ring, fd.as_fd(), &mut buffer, IoringSqeFlags::empty()).await {
						Ok([]) => {
							assert_eq!(total_read, group.info.size);
							break;
						}
						Ok(x) => {
							total_read += <usize as TryInto<u64>>::try_into(x.len()).unwrap();
							assert!(total_read <= group.info.size);
							hash.write(x);
						}
						Err(Errno::AGAIN | Errno::INTR | Errno::CANCELED) => (),
						Err(x) => panic!("{:?}", x),
					}
				}

				let _ = Fadvise::new(
					ring,
					fd.as_fd(),
					0,
					0,
					Advice::DontNeed,
					IoringSqeFlags::empty(),
				)
				.await;

				let _ = Close::new(ring, fd).await;

				file.hash = hash.finish();
			})
		})
		.collect();

	SliceJoin(&mut files).await;
	drop(files);

	group.files.sort_unstable();

	for chunk in group.files.chunk_by(|a, b| a.hash == b.hash) {
		if chunk.len() <= 1 {
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

struct IteratorJoin<const N: usize, F: Future<Output = ()>, I: Iterator<Item = F>> {
	buffer: [Option<F>; N],
	iter: Fuse<I>,
}

impl<const N: usize, F: Future<Output = ()>, I: Iterator<Item = F>> IteratorJoin<N, F, I> {
	fn new(iter: I) -> Self {
		Self {
			buffer: [const { None }; N],
			iter: iter.fuse(),
		}
	}
}

impl<const N: usize, F: Future<Output = ()>, I: Iterator<Item = F>> Future
	for IteratorJoin<N, F, I>
{
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
