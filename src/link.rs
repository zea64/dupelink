use core::{ffi::CStr, fmt::Write};
use std::ffi::CString;

use rustix::fs::{linkat, renameat_with, unlink, AtFlags, RenameFlags, CWD};

use crate::{FileGroup, Globals, LinkMethod, Path};

const TMP_FILE_NAME: &CStr = c"DUPELINK_TMP_FILE";

pub fn link(
	globals: &Globals,
	groups: Vec<FileGroup>,
	total_deduped_files: &mut u64,
	total_deduped_size: &mut u64,
) {
	let mut buffer = String::new();
	for mut group in groups {
		buffer.clear();

		let nr_deduped_files: u64 = (group.files.len() - 1).try_into().unwrap();
		*total_deduped_files += nr_deduped_files;
		let deduped_size = nr_deduped_files * group.info.size;
		*total_deduped_size += deduped_size;

		writeln!(
			&mut buffer,
			"{} bytes each, {} bytes deduped",
			group.info.size, deduped_size,
		)
		.unwrap();

		// Sort by ctime
		group.files.sort_unstable_by(|a, b| a.ctime.cmp(&b.ctime));

		match globals.link_method {
			LinkMethod::Hardlink => hardlink(&mut buffer, group, globals.link),
			LinkMethod::Reflink => reflink(&mut buffer, group, globals.link),
		}

		println!("{}", buffer);
	}
}

fn hardlink(write_buffer: &mut impl Write, group: FileGroup, link: bool) {
	let mut names = group.files.iter().flat_map(|file| file.path.iter());
	let master_name = {
		let mut buf = Vec::new();
		names.next().unwrap().flatten(&mut buf);
		CString::from_vec_with_nul(buf).unwrap()
	};

	let mut tmp_path_buf = Vec::new();
	let mut orig_path_buf = Vec::new();

	writeln!(write_buffer, "{:?}", master_name).unwrap();

	for path in names {
		// TODO, check that file hasn't changed.

		let name = path.flatten(&mut orig_path_buf);

		if link {
			let tmp_path = path
				.prefix
				.as_ref()
				.map(|x| Path::extend(x, TMP_FILE_NAME))
				.unwrap_or_else(|| Path::new(TMP_FILE_NAME));

			let tmp_name = tmp_path.flatten(&mut tmp_path_buf);

			if let Err(err) = linkat(CWD, &master_name, CWD, tmp_name, AtFlags::empty()) {
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

		writeln!(write_buffer, "{:?}", name).unwrap();
	}
}

fn reflink(_write_buffer: &mut impl Write, _group: FileGroup, _link: bool) {
	unimplemented!()
}
