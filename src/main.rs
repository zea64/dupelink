#![feature(hash_set_entry)]

use core::{
	cell::RefCell,
	ffi::CStr,
	fmt,
	future::Future,
	hash::{Hash, Hasher},
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
	fs::{openat2, AtFlags, Dir, FileType, Mode, OFlags, ResolveFlags, StatxFlags, CWD},
	io_uring::IoringSqeFlags,
};
use uring_async::{block_on, ops::Statx, Uring};

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

fn is_all_same_inode(files: &[FileInfo]) -> bool {
	let last_ino = files[0].ino;
	for file in files.iter().skip(1) {
		if file.ino != last_ino {
			return false;
		}
	}
	true
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

fn main() {
	let ring = RefCell::new(Uring::new().unwrap());
	let mut map = HashMap::new();

	for path in std::env::args_os().skip(1) {
		let mut path = path.into_vec();
		path.push(0);
		let path = unsafe { CString::from_vec_with_nul_unchecked(path) };

		let dir = openat2(
			CWD,
			&path,
			OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOCTTY | OFlags::CLOEXEC,
			Mode::empty(),
			ResolveFlags::empty(),
		)
		.unwrap();

		recurse_dir(&ring, dir, &path, &mut map);
	}

	let groups: Vec<FileGroup> = map
		.into_iter()
		.filter_map(|(k, mut v)| {
			if v.len() <= 1 || is_all_same_inode(&v) {
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

	//println!("{:?}", groups);

	let mut new_groups: Vec<FileGroup> = Default::default();

	for mut group in groups {
		// Hash files
		for file in group.files.iter_mut() {
			println!("Reading {:?}", file.path.first_name());
			file.hash = 0;
		}

		group.files.sort_unstable();

		for chunk in group.files.chunk_by(|a, b| a.hash == b.hash) {
			if chunk.len() <= 1 {
				continue;
			}
			new_groups.push(FileGroup {
				info: group.info,
				files: chunk.to_owned(),
			});
		}
	}

	println!("{:#?}", new_groups);
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
						StatxFlags::BASIC_STATS,
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

	block_on(ring, SliceJoin { slice: &mut files });

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
		let full_path = unsafe { CString::from_vec_with_nul_unchecked(path_buf.clone()) };
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

enum FutureOrOutput<F, O> {
	Future(F),
	Output(O),
}

struct SliceJoin<'a, F: Future> {
	slice: &'a mut [FutureOrOutput<F, <F as Future>::Output>],
}

impl<'a, F: Future> Future for SliceJoin<'a, F> {
	type Output = ();

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = Pin::into_inner(self);
		let mut output_count = 0;
		for fut_or_output in this.slice.iter_mut() {
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

		if output_count == this.slice.len() {
			Poll::Ready(())
		} else {
			Poll::Pending
		}
	}
}

#[derive(Clone)]
struct MegaName(Box<[u8]>);

impl<'a> MegaName {
	fn from_cstring(cstring: CString) -> Self {
		Self(cstring.into_bytes().into())
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

	unsafe { CStr::from_bytes_with_nul_unchecked(path_buf) }
}
