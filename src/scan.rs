use core::{cell::Cell, error::Error, fmt, hash::Hasher, mem::MaybeUninit, time::Duration};
use std::{hash::DefaultHasher, rc::Rc, time::SystemTime};

use rustix::{
	fd::{AsRawFd, BorrowedFd, OwnedFd},
	fs::{openat2, AtFlags, Dir, FileType, Mode, Statx as StatxStruct, StatxFlags},
	io::Errno,
};
use uring_async::ops::Statx;

use crate::{
	block_on,
	future::{FutureOrOutput, SliceJoin},
	FileInfo,
	FileMap,
	Globals,
	GroupInfo,
	Path,
};

pub fn recurse_dir(
	globals: &Globals,
	dirfd: OwnedFd,
	dir_path: Rc<Path>,
	map: &mut FileMap,
	scanned_files: &Cell<usize>,
) {
	// # SAFETY
	// We only use this as dfd in openat while dir_iter is still alive.
	let borrowed_dir = unsafe { BorrowedFd::borrow_raw(dirfd.as_raw_fd()) };
	let dir = Dir::new(dirfd).unwrap();
	let mut dir_iter = dir.skip(2);

	let mut file_names = Vec::new();
	let mut dir_names = Vec::new();

	for dentry in &mut dir_iter {
		let dentry = dentry.unwrap();
		let name = dentry.file_name().to_owned();

		match dentry.file_type() {
			FileType::RegularFile => file_names.push(name),
			FileType::Directory => dir_names.push(name),
			_ => (),
		}
	}

	file_names.sort_unstable();
	let mut files: Vec<_> = file_names
		.into_iter()
		.map(|name| {
			FutureOrOutput::Future(async move {
				let mut statx_buf = unsafe { MaybeUninit::zeroed().assume_init() };
				let ret = Statx::new(
					&globals.ring,
					borrowed_dir,
					&name,
					AtFlags::empty(),
					globals.statx_flags,
					&mut statx_buf,
				)
				.await;
				(ret.map(|_| statx_buf), name)
			})
		})
		.collect();

	scanned_files.set(scanned_files.get() + files.len());

	block_on(
		&globals.ring,
		&|| eprint!("{}", format!("Scanning {}...\r", scanned_files.get())),
		SliceJoin::new(&mut files),
	);

	for output in files {
		let (statx, file_path) = output.unwrap_output();
		let statx = match statx {
			Ok(statx) => statx,
			Err(err) => {
				eprintln!("Error statting {:?}: {}", file_path, err);
				continue;
			}
		};

		if let Err(err) = record_stat(globals, map, statx, Path::extend(&dir_path, &file_path)) {
			eprintln!("Error statting {:?}: {}", file_path, err)
		}
	}

	dir_names.sort_unstable();
	dir_names.shrink_to_fit();

	for new_dir_path in dir_names {
		let path = Path::extend(&dir_path, &new_dir_path);
		match openat2(
			borrowed_dir,
			&new_dir_path,
			globals.dir_oflags,
			Mode::empty(),
			globals.dir_open_how.resolve,
		) {
			Ok(new_dirfd) => recurse_dir(globals, new_dirfd, Rc::new(path), map, scanned_files),
			Err(Errno::XDEV) => (),
			Err(err) => {
				eprintln!("Error opening {}: {}", path, err);
				continue;
			}
		}
	}
}

#[derive(Debug)]
pub struct RecordStatError {
	expected: StatxFlags,
	got: StatxFlags,
}

impl Error for RecordStatError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		None
	}
}

impl fmt::Display for RecordStatError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Expected statx flags {:#8x}, got {:#8x} (diff={:#8x})",
			self.expected,
			self.got,
			self.expected - self.got
		)
	}
}

pub fn record_stat(
	globals: &Globals,
	map: &mut FileMap,
	statx: StatxStruct,
	path: Path,
) -> Result<(), RecordStatError> {
	let set_flags = StatxFlags::from_bits_truncate(statx.stx_mask);
	if !set_flags.contains(globals.statx_flags) {
		return Err(RecordStatError {
			expected: globals.statx_flags,
			got: set_flags,
		});
	}

	if statx.stx_size < globals.minsize || statx.stx_size > globals.maxsize {
		return Ok(());
	}

	let mut hasher = DefaultHasher::new();
	hasher.write_u16(statx.stx_mode);
	hasher.write_u32(statx.stx_uid);
	hasher.write_u32(statx.stx_gid);
	hasher.write_u64(statx.stx_mnt_id);

	let group_info = GroupInfo {
		size: statx.stx_size,
		hashed: hasher.finish(),
	};

	let file_info = FileInfo {
		ino: statx.stx_ino,
		hash: 0,
		ctime: SystemTime::UNIX_EPOCH
			+ Duration::from_secs(statx.stx_ctime.tv_sec.try_into().unwrap())
			+ Duration::from_nanos(statx.stx_ctime.tv_nsec.into()),
		path: vec![path],
	};

	map.entry(group_info).or_default().push(file_info);
	Ok(())
}
