#![feature(slice_split_once, noop_waker)]
#![allow(clippy::format_in_format_args)]

mod future;
mod link;
mod read;
mod scan;

use core::{
	cell::{Cell, RefCell},
	cmp,
	ffi::CStr,
	fmt,
	hash::Hash,
	mem,
	num::ParseIntError,
};
use std::{
	collections::HashMap,
	ffi::CString,
	os::unix::ffi::OsStringExt,
	rc::Rc,
	time::SystemTime,
};

use future::{block_on, IteratorJoin};
use rustix::{
	fs::{self, openat2, AtFlags, Mode, OFlags, ResolveFlags, StatxFlags, CWD},
	io::Errno,
	io_uring::open_how,
};
use uring_async::{sync::Semaphore, Uring};

#[derive(Debug, Clone)]
struct FileInfo {
	ino: u64,
	hash: u64,
	ctime: SystemTime,
	path: Vec<Path>,
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

#[derive(Debug, Clone, Copy)]
enum LinkMethod {
	Hardlink,
	Reflink,
}

#[derive(Debug)]
struct FileGroup {
	info: GroupInfo,
	files: Vec<FileInfo>,
}

type FileMap = HashMap<GroupInfo, Vec<FileInfo>>;

#[derive(Debug)]
struct Args {
	minsize: u64,
	maxsize: u64,
	fds: usize,
	noatime: bool,
	link: bool,
	link_method: LinkMethod,
	paths: Vec<CString>,
}

const HELP_STR: &str = "\
USAGE: dupelink [OPTIONS] <PATHS>...

ARGS:
	<PATHS...>             Paths to search for duplicates

OPTIONS:
	-l, --link             Link duplicate files (otherwise only print them)
	-R, --reflink          Use reflinks instead of hardlinks (also allows deduping files with different metadata)
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
		link_method: LinkMethod::Hardlink,
		paths: Vec::new(),
	};

	let mut parser = lexopt::Parser::from_env();
	while let Some(arg) = parser.next()? {
		match arg {
			Short('l') | Long("link") => {
				args.link = true;
			}
			Short('R') | Long("reflink") => {
				args.link_method = LinkMethod::Reflink;
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
	link_method: LinkMethod,
	link: bool,
	statx_flags: StatxFlags,
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
	let statx_flags = StatxFlags::INO
		| StatxFlags::TYPE
		| StatxFlags::MNT_ID
		| StatxFlags::CTIME
		| match args.link_method {
			LinkMethod::Hardlink => StatxFlags::MODE | StatxFlags::UID | StatxFlags::GID,
			LinkMethod::Reflink => StatxFlags::empty(),
		};

	let globals = Globals {
		ring,
		link_method: args.link_method,
		link: args.link,
		statx_flags,
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
			Ok(dir) => {
				scan::recurse_dir(
					&globals,
					dir,
					Rc::new(Path::new(path)),
					&mut map,
					&Cell::new(0),
				);
			}
			Err(Errno::NOTDIR) => {
				match fs::statx(CWD, path, AtFlags::empty(), globals.statx_flags) {
					Ok(stat) => {
						if let Err(err) =
							scan::record_stat(&globals, &mut map, stat, Path::new(path))
						{
							eprintln!("Error statting {:?}: {}", path, err);
						}
					}
					Err(err) => eprintln!("Error statting {:?}: {}", path, err),
				}
			}
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
						path: infos
							.iter_mut()
							.flat_map(|x| mem::take(&mut x.path))
							.collect(),
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
		&|| print_progress(&completion_count, inodes_small_count),
		IteratorJoin::<_, _>::new(
			globals.fds,
			groups.into_iter().map(|group| {
				read::hash_group(
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

	let inodes_final_count = groups_small_read
		.borrow()
		.iter()
		.flat_map(|x| x.files.iter())
		.count();

	eprintln!("\n{} inodes left (small read)\n", inodes_final_count,);

	let completion_count = Cell::new(0usize);
	block_on(
		&globals.ring,
		&|| print_progress(&completion_count, inodes_final_count),
		IteratorJoin::<_, _>::new(
			globals.fds,
			groups_small_read.into_inner().into_iter().map(|group| {
				read::hash_group(
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

	let mut total_deduped_files: u64 = 0;
	let mut total_deduped_size: u64 = 0;

	link::link(
		&globals,
		groups_final.into_inner(),
		&mut total_deduped_files,
		&mut total_deduped_size,
	);

	println!(
		"\nSummary:\n{} files totalling {} bytes",
		total_deduped_files, total_deduped_size
	);
}

fn print_progress(cur: &Cell<usize>, total: usize) {
	let cur = cur.get();
	let integer_part = cur * 100 / total;
	let frac_part = cur * 10000 / total - integer_part * 100;

	eprint!(
		"{}",
		format!("\r[{cur}/{total}] ({integer_part:.2}.{frac_part:.2}%)")
	);
}

#[derive(Debug, Clone)]
struct Path {
	suffix: Box<[u8]>,
	prefix: Option<Rc<Path>>,
}

impl<'a> Path {
	fn new(suffix: &CStr) -> Self {
		Path {
			suffix: suffix.to_bytes().to_owned().into(),
			prefix: None,
		}
	}

	fn extend(self: &Rc<Self>, suffix: &CStr) -> Self {
		Path {
			suffix: suffix.to_bytes().to_owned().into(),
			prefix: Some(self.clone()),
		}
	}

	fn flatten(&self, buf: &'a mut Vec<u8>) -> &'a CStr {
		buf.clear();

		fn inner(s: &Path, buf: &mut Vec<u8>) {
			if let Some(ref s) = s.prefix {
				inner(s, buf);
				buf.push(b'/');
			}
			buf.extend_from_slice(&s.suffix);
		}

		inner(self, buf);
		buf.push(0);

		CStr::from_bytes_until_nul(buf).unwrap()
	}
}

impl fmt::Display for Path {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if let Some(ref prefix) = self.prefix {
			write!(f, "{}/", prefix)?;
		}

		write!(f, "{}", core::str::from_utf8(&self.suffix).unwrap_or("?"))
	}
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
