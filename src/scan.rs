use core::{ffi::CStr, hash::Hasher, mem::MaybeUninit, time::Duration};
use std::{collections::HashMap, ffi::CString, time::SystemTime};

use rustix::{
	fd::{AsFd, BorrowedFd, OwnedFd},
	fs::{openat2, AtFlags, Dir, FileType, Mode, StatxFlags},
	io::Errno,
};
use uring_async::ops::Statx;

use crate::{
	block_on,
	path_concat,
	FileInfo,
	FutureOrOutput,
	Globals,
	GroupInfo,
	MegaName,
	SliceJoin,
};

pub fn recurse_dir(
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
					let mut statx_buf = unsafe { MaybeUninit::zeroed().assume_init() };
					let ret = Statx::new(
						&globals.ring,
						dirfd_borrow,
						&file_name,
						AtFlags::empty(),
						StatxFlags::INO
							| StatxFlags::TYPE | StatxFlags::MODE
							| StatxFlags::UID | StatxFlags::GID
							| StatxFlags::MNT_ID | StatxFlags::CTIME,
						&mut statx_buf,
					)
					.await;

					(ret.map(|_| statx_buf), file_name)
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
			hash: 0,
			ctime: SystemTime::UNIX_EPOCH
				+ Duration::from_secs(statx.stx_ctime.tv_sec.try_into().unwrap())
				+ Duration::from_nanos(statx.stx_ctime.tv_nsec.into()),
			path: MegaName::from_cstring(full_path),
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
