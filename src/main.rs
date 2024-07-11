#![feature(hash_set_entry)]

use core::{
	cell::RefCell,
	ffi::CStr,
	future::Future,
	hash::{Hash, Hasher},
	mem::MaybeUninit,
	pin::Pin,
};
use std::{
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

#[derive(Debug)]
struct FileInfo {
	ino: u64,
	path: CString,
}

impl PartialEq for FileInfo {
	fn eq(&self, other: &Self) -> bool {
		self.ino == other.ino
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

impl FileGroup {
	fn add(&mut self, file: FileInfo) {
		self.files.push(file);
	}
}

impl Hash for FileGroup {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.info.hash(state);
	}
}

impl PartialEq for FileGroup {
	fn eq(&self, other: &Self) -> bool {
		self.info == other.info
	}
}

impl Eq for FileGroup {}

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

	println!("{:#?}", map);
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
			path: full_path,
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

fn path_concat<'a, 'b>(path_buf: &'a mut Vec<u8>, str1: &'b CStr, str2: &'b CStr) -> &'a CStr {
	path_buf.clear();
	path_buf.extend_from_slice(str1.to_bytes());
	path_buf.push(b'/');
	path_buf.extend_from_slice(str2.to_bytes());
	path_buf.push(0);

	unsafe { CStr::from_bytes_with_nul_unchecked(path_buf) }
}
