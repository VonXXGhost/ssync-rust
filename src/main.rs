use std::{fs, io, thread};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::os::windows::prelude::*;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use clap::Parser;
use config::{Config, Value};
use filetime::FileTime;
use regex::Regex;

fn main() -> Result<()> {
    let args = Args::parse();
    let context = read_config(&args.file)?;

    println!("加载配置: {:#?}", context);
    let (src_dict_info, to_dict_info) = get_dict_info(&context);
    println!("已加载目录信息");

    let decision_result = DecisionTask::new(
        Arc::new(src_dict_info),
        Arc::new(to_dict_info),
        Arc::new(context),
    ).make_decision();

    println!("{}", decision_result);

    if decision_result.is_empty() {
        println!("风平浪静，下次再见");
        exit(0);
    }

    check_continue("继续执行文件操作？");

    DecisionExecuteTask::new(decision_result).execute();

    ready_to_exit();
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(version, about = "简单的本地文件同步", long_about = None)]
struct Args {
    /// 配置文件路径
    #[clap(default_value_t = String::from("ssync.yml"), short, long, value_parser)]
    file: String,
}

#[derive(Debug, Clone)]
struct SyncPath {
    /// 目录路径
    path: String,
    /// 白名单正则
    include: Vec<Regex>,
    /// 排除正则
    exclude: Vec<Regex>,
}

#[derive(Debug, Clone)]
struct SyncContext {
    /// 被同步目录信息
    from: SyncPath,
    /// 目标目录信息
    to: SyncPath,
    /// 是否递归子文件夹
    recursive: bool,
}

#[derive(Debug)]
struct FileInfo {
    /// 文件名或目录名
    name: String,
    /// 顶层目录路径
    root: String,
    /// 绝对路径（不含本文件/目录名）
    absolute_dir: String,
}

impl FileInfo {
    pub fn new(name: String, root: String, absolute_dir: String) -> Self {
        Self {
            name,
            root,
            absolute_dir,
        }
    }

    fn absolute_dir_with_self(&self) -> String {
        String::from(
            Path::new(&self.absolute_dir).join(&self.name).as_path().to_str().unwrap()
        )
    }

    fn file(&self) -> File {
        File::open(self.absolute_dir_with_self()).unwrap()
    }

    fn relative_path(&self) -> String {
        String::from(
            pathdiff::diff_paths(self.absolute_dir_with_self(), &self.root).unwrap()
                .as_path().to_str().unwrap()
        )
    }

    fn relative_path_without_file(&self) -> String {
        String::from(
            pathdiff::diff_paths(self.absolute_dir_with_self(), &self.root).unwrap()
                .as_path().parent().unwrap().to_str().unwrap()
        )
    }

    fn to_path(&self) -> PathBuf {
        Path::new(&self.absolute_dir_with_self()).to_path_buf()
    }
}

#[derive(Debug)]
struct DirectoryInfo {
    /// 顶层目录路径
    root: String,
    /// 绝对路径（包含自己）
    absolute_dir: String,
    /// 子文件夹列表
    sub_dirs: Vec<Arc<DirectoryInfo>>,
    /// 文件列表
    files: Vec<Arc<FileInfo>>,
}

impl DirectoryInfo {
    fn create(root: String, absolute_dir: String) -> Self {
        Self {
            root,
            absolute_dir,
            sub_dirs: Vec::new(),
            files: Vec::new(),
        }
    }

    fn load_all_file(absolute_path: String, recursive: bool,
                     root_dir: String, context: &SyncContext,
                     direction: &OperateDirection) -> Result<DirectoryInfo> {
        // 保证path为绝对路径
        let path = fs::canonicalize(Path::new(absolute_path.as_str()))?;
        let absolute_path = path.to_str().unwrap().to_string();
        let root_dir = fs::canonicalize(Path::new(root_dir.as_str()))?
            .to_str().unwrap().to_string();
        let mut directory_info = DirectoryInfo::create(root_dir.clone(), absolute_path);
        if !path.exists() || !path.is_dir() {
            return Ok(directory_info);
        }
        assert!(!(recursive && root_dir.is_empty()), "root_dir can not be empty when recursive is true");
        for entry in fs::read_dir(path)? {
            let path = entry?.path();
            let abs_path = path.to_str().unwrap();
            if !DirectoryInfo::_check_include_and_exclude(abs_path, context, direction) {
                continue;
            }
            if path.is_dir() {
                let dict_info = if recursive {
                    DirectoryInfo::load_all_file(abs_path.to_string(),
                                                 recursive,
                                                 root_dir.clone(),
                                                 context,
                                                 direction)?
                } else {
                    DirectoryInfo::create(root_dir.clone(), abs_path.to_string())
                };
                directory_info.sub_dirs.push(Arc::new(dict_info));
            } else {
                let file_info = FileInfo::new(
                    path.file_name().unwrap().to_str().unwrap().to_string(),
                    root_dir.clone(),
                    path.parent().unwrap().to_str().unwrap().to_string(),
                );
                directory_info.files.push(Arc::new(file_info));
            }
        }

        return Ok(directory_info);
    }

    fn _check_include_and_exclude(abs_path: &str,
                                  context: &SyncContext,
                                  direction: &OperateDirection) -> bool {
        match direction {
            OperateDirection::FROM => {
                for reg in &context.from.include {
                    if reg.is_match(abs_path) {
                        return true;
                    }
                }
                if !&context.from.include.is_empty() {
                    return false;
                }
                for reg in &context.from.exclude {
                    if reg.is_match(abs_path) {
                        return false;
                    }
                }
                true
            }
            OperateDirection::TO => {
                for reg in &context.to.include {
                    if reg.is_match(abs_path) {
                        return true;
                    }
                }
                if !&context.to.include.is_empty() {
                    return false;
                }
                for reg in &context.to.exclude {
                    if reg.is_match(abs_path) {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn name(&self) -> String {
        String::from(
            Path::new(&self.absolute_dir).file_name().unwrap().to_str().unwrap()
        )
    }

    fn to_file_info(&self) -> FileInfo {
        FileInfo::new(
            self.name().clone(),
            self.root.clone(),
            Path::new(&self.absolute_dir).parent().unwrap().to_str().unwrap().to_string(),
        )
    }

    fn relative_path(&self) -> String {
        String::from(
            pathdiff::diff_paths(&self.absolute_dir, &self.root).unwrap()
                .as_path().to_str().unwrap()
        )
    }
}

enum OperateDirection {
    FROM,
    TO,
}

#[derive(Debug)]
enum FileAction {
    ADD,
    DEL,
    UPDATE,
}

#[derive(Debug)]
struct DecisionResultItem {
    action: FileAction,
    // 操作为删除时，没有src
    src_file_info: Option<Arc<FileInfo>>,
    dest_file_info: Arc<FileInfo>,
}

#[derive(Debug)]
struct DecisionResult {
    add_items: HashMap<String, Vec<DecisionResultItem>>,
    del_items: HashMap<String, Vec<DecisionResultItem>>,
    update_items: HashMap<String, Vec<DecisionResultItem>>,
}

impl DecisionResult {
    fn new() -> Self {
        Self {
            add_items: HashMap::new(),
            del_items: HashMap::new(),
            update_items: HashMap::new(),
        }
    }

    fn total_count(&self) -> usize {
        let mut cnt = 0;
        self.add_items.values().for_each(|x| cnt += x.len());
        self.del_items.values().for_each(|x| cnt += x.len());
        self.update_items.values().for_each(|x| cnt += x.len());
        cnt
    }

    fn summary(&self) -> String {
        if self.is_empty() {
            return String::from("无任务需执行");
        }
        let mut summary = String::new();

        fn print_func(items: &Vec<DecisionResultItem>, summary: &mut String) {
            for item in items {
                summary.push('\t');
                summary.push_str(&item.dest_file_info.relative_path());
                summary.push('\n');
            }
        }

        summary.push_str("——分析结果——\n");
        summary.push_str("· 新增：\n");
        self.add_items.values().for_each(|items| print_func(items, &mut summary));
        if self.add_items.is_empty() {
            summary.pop();
            summary.push_str("\t无\n");
        }

        summary.push_str("· 删除：\n");
        self.del_items.values().for_each(|items| print_func(items, &mut summary));
        if self.del_items.is_empty() {
            summary.pop();
            summary.push_str("无\n");
        }

        summary.push_str("· 更新：\n");
        self.update_items.values().for_each(|items| print_func(items, &mut summary));
        if self.update_items.is_empty() {
            summary.pop();
            summary.push_str("无\n");
        }

        return summary;
    }

    fn merge(&mut self, other: DecisionResult) {
        self.add_items.extend(other.add_items);
        self.del_items.extend(other.del_items);
        self.update_items.extend(other.update_items);
    }

    fn is_empty(&self) -> bool {
        self.total_count() == 0
    }
}

impl Display for DecisionResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary())
    }
}

#[derive(Debug)]
struct DecisionTask {
    from_dict_info: Arc<DirectoryInfo>,
    to_dict_info: Arc<DirectoryInfo>,
    context: Arc<SyncContext>,

    _decision_result: DecisionResult,
    _from_file_names: HashMap<String, Arc<FileInfo>>,
    _to_file_names: HashMap<String, Arc<FileInfo>>,
    _from_dict_names: HashMap<String, Arc<DirectoryInfo>>,
    _to_dict_names: HashMap<String, Arc<DirectoryInfo>>,
}

impl DecisionTask {
    pub fn new(from_dict_info: Arc<DirectoryInfo>, to_dict_info: Arc<DirectoryInfo>, context: Arc<SyncContext>) -> Self {
        Self {
            from_dict_info: from_dict_info.clone(),
            to_dict_info: to_dict_info.clone(),
            context,
            _decision_result: DecisionResult::new(),
            _from_file_names: from_dict_info.files.iter()
                .map(|x| (x.name.clone(), x.clone()))
                .collect(),
            _to_file_names: to_dict_info.files.iter()
                .map(|x| (x.name.clone(), x.clone()))
                .collect(),
            _from_dict_names: from_dict_info.sub_dirs.iter()
                .map(|x| (x.name(), x.clone()))
                .collect(),
            _to_dict_names: to_dict_info.sub_dirs.iter()
                .map(|x| (x.name(), x.clone()))
                .collect(),
        }
    }

    fn make_decision(mut self) -> DecisionResult {
        self._decision_result.add_items
            .insert(self.from_dict_info.relative_path(), self.find_add());

        self._decision_result.del_items
            .insert(self.from_dict_info.relative_path(), self.find_del());

        self._decision_result.update_items
            .insert(self.from_dict_info.relative_path(), self.find_update());

        for (sub_src, sub_dest) in self.find_both_sub_dirs() {
            let sub_result = DecisionTask::new(
                sub_src.clone(),
                sub_dest.clone(),
                self.context.clone(),
            ).make_decision();
            self._decision_result.merge(sub_result);
        }

        self._decision_result
    }

    fn find_both_sub_dirs(&self) -> Vec<(Arc<DirectoryInfo>, Arc<DirectoryInfo>)> {
        self.from_dict_info.sub_dirs.iter()
            .filter(|it| self._to_dict_names.contains_key(it.name().as_str()))
            .map(|it| (it.clone(), self._to_dict_names.get(it.name().as_str()).unwrap().clone()))
            .collect()
    }

    /// 只根据文件名/目录名判断，源目录下有，新目录下没有，就新增
    fn find_add(&self) -> Vec<DecisionResultItem> {
        let mut add_items = Vec::new();
        // 判断目录
        if self.context.recursive {
            for it in self.from_dict_info.sub_dirs.iter() {
                if !self._to_dict_names.contains_key(it.name().as_str()) {
                    add_items.push(DecisionResultItem {
                        action: FileAction::ADD,
                        src_file_info: Some(Arc::new(it.to_file_info())),
                        dest_file_info: Arc::new(self.gene_add_dest_file_info(&it.to_file_info())),
                    })
                }
            }
        }
        // 判断文件
        for it in self.from_dict_info.files.iter() {
            if !self._to_file_names.contains_key(&it.name) {
                add_items.push(
                    DecisionResultItem {
                        action: FileAction::ADD,
                        src_file_info: Some(it.clone()),
                        dest_file_info: Arc::new(self.gene_add_dest_file_info(&it)),
                    }
                );
            }
        }
        add_items
    }

    /// 只根据文件名/目录名判断，源目录下没有，新目录下有，就删除
    fn find_del(&self) -> Vec<DecisionResultItem> {
        let mut items = Vec::new();
        // 判断目录
        if self.context.recursive {
            for it in self.to_dict_info.sub_dirs.iter() {
                if !self._from_dict_names.contains_key(it.name().as_str()) {
                    items.push(DecisionResultItem {
                        action: FileAction::DEL,
                        src_file_info: None,
                        dest_file_info: Arc::new(it.to_file_info()),
                    })
                }
            }
        }
        // 判断文件
        for it in self.to_dict_info.files.iter() {
            if !self._from_file_names.contains_key(&it.name) {
                items.push(
                    DecisionResultItem {
                        action: FileAction::DEL,
                        src_file_info: None,
                        dest_file_info: it.clone(),
                    }
                );
            }
        }
        items
    }

    /// 根据配置判断更新了的文件。先看修改时间，不一致再看文件内容。
    /// 因为新增、删除在其他任务里了，这里只需要管两边都有的文件即可
    fn find_update(&self) -> Vec<DecisionResultItem> {
        let mut items = Vec::new();
        for it in self.to_dict_info.files.iter() {
            if !self._from_file_names.contains_key(&it.name) {
                continue;
            }
            let src_file_info = self._from_file_names.get(&it.name).unwrap().clone();
            if Self::check_has_updated(&src_file_info, it) {
                items.push(DecisionResultItem {
                    action: FileAction::UPDATE,
                    src_file_info: Some(src_file_info.clone()),
                    dest_file_info: it.clone(),
                });
            }
        }
        items
    }

    fn check_has_updated(src: &FileInfo, dest: &FileInfo) -> bool {
        let src = src.file();
        let dest = dest.file();
        return src.metadata().unwrap().last_write_time() != dest.metadata().unwrap().last_write_time()
            && !is_same_file(&src, &dest);
    }

    fn gene_add_dest_file_info(&self, src: &FileInfo) -> FileInfo {
        // 关键在于根据相对目录生成目标的绝对目录
        let mut absolute_path = PathBuf::from(&self.to_dict_info.root);
        absolute_path.push(Path::new(&src.relative_path_without_file()));
        return FileInfo::new(
            src.name.clone(),
            self.to_dict_info.root.clone(),
            absolute_path.to_str().unwrap().to_string(),
        );
    }
}

struct DecisionExecuteTask {
    decision: DecisionResult,

    _total_count: usize,
    _processed_count: AtomicUsize,
}

impl DecisionExecuteTask {
    pub fn new(decision: DecisionResult) -> Self {
        let total_count = decision.total_count();
        Self {
            decision,
            _total_count: total_count,
            _processed_count: AtomicUsize::new(0),
        }
    }

    pub fn execute(self) {
        println!("同步任务开始执行");
        self.execute_add_task();
        self.execute_update_task();
        self.execute_del_task();
        println!("同步任务执行完毕");
    }

    fn log_progress(&self, counter: &AtomicUsize, item: &DecisionResultItem) {
        match item.action {
            FileAction::ADD => {
                let prefix = self.count_and_progress_prefix(counter);
                println!("{}  Copying - {} to {}", prefix,
                         adjust_canonicalization(item.src_file_info.as_ref().unwrap()
                             .absolute_dir_with_self()),
                         adjust_canonicalization(item.dest_file_info.absolute_dir_with_self())
                );
            }
            FileAction::DEL => {
                let prefix = self.count_and_progress_prefix(counter);
                println!("{}  Deleting - {}", prefix,
                         adjust_canonicalization(item.dest_file_info.absolute_dir_with_self())
                );
            }
            FileAction::UPDATE => {
                let prefix = self.count_and_progress_prefix(counter);
                println!("{}  Updating - {} to {}", prefix,
                         adjust_canonicalization(item.src_file_info.as_ref().unwrap()
                             .absolute_dir_with_self()),
                         adjust_canonicalization(item.dest_file_info.absolute_dir_with_self())
                );
            }
        }
    }

    fn count_and_progress_prefix(&self, counter: &AtomicUsize) -> String {
        let cnt = counter.fetch_add(1, Ordering::Relaxed);
        return format!("{}/{}", cnt, self._total_count);
    }

    fn execute_add_task(&self) {
        for (_, items) in &self.decision.add_items {
            for it in items {
                self.log_progress(&self._processed_count, it);
                copy_recursively(
                    Path::new(&it.src_file_info.as_ref().unwrap().absolute_dir_with_self()),
                    Path::new(&it.dest_file_info.absolute_dir_with_self()),
                    false
                ).unwrap();
            }
        }
    }

    fn execute_del_task(&self) {
        for (_, items) in &self.decision.del_items {
            for it in items {
                self.log_progress(&self._processed_count, it);
                let path = it.dest_file_info.to_path();
                if path.is_dir() {
                    fs::remove_dir_all(path)
                } else {
                    fs::remove_file(path)
                }.unwrap();
            }
        }
    }

    fn execute_update_task(&self) {
        for (_, items) in &self.decision.update_items {
            for it in items {
                self.log_progress(&self._processed_count, it);
                copy_recursively(
                    Path::new(&it.src_file_info.as_ref().unwrap().absolute_dir_with_self()),
                    Path::new(&it.dest_file_info.absolute_dir_with_self()),
                    true
                ).unwrap();
            }
        }
    }
}

// Function

/// 读取配置文件
fn read_config(file_path: &str) -> Result<SyncContext> {
    let settings = Config::builder()
        .add_source(config::File::with_name(file_path))
        .build()?;

    let mut from_settings = settings.get_table("from")?;
    let mut to_settings = settings.get_table("to")?;

    fn to_regex_vec(value_vec: Option<Value>) -> Result<Vec<Regex>> {
        Ok(match value_vec {
            Some(value) => {
                let mut vec = Vec::new();
                for v in value.into_array()? {
                    let reg_str = v.into_string()?;
                    vec.push(Regex::new(reg_str.as_str())?)
                }
                vec
            }
            None => vec![]
        })
    }

    return Ok(SyncContext {
        from: SyncPath {
            path: from_settings.remove("path").unwrap().into_string()?,
            include: to_regex_vec(from_settings.remove("include"))?,
            exclude: to_regex_vec(from_settings.remove("exclude"))?,
        },
        to: SyncPath {
            path: to_settings.remove("path").unwrap().into_string()?,
            include: to_regex_vec(to_settings.remove("include"))?,
            exclude: to_regex_vec(to_settings.remove("exclude"))?,
        },
        recursive: settings.get_bool("recursive").unwrap_or(false),
    });
}

fn get_dict_info(sync_context: &SyncContext) -> (DirectoryInfo, DirectoryInfo) {
    let (stx, srx) = mpsc::channel();
    let (ttx, trx) = mpsc::channel();

    let context = sync_context.clone();
    thread::spawn(move || {
        let src_dict_info = DirectoryInfo::load_all_file(
            context.from.path.clone(),
            true,
            context.from.path.clone(),
            &context,
            &OperateDirection::FROM,
        ).expect("src_dict_info can not load");
        stx.send(src_dict_info).unwrap();
    });

    let context = sync_context.clone();
    thread::spawn(move || {
        let to_dict_info = DirectoryInfo::load_all_file(
            context.to.path.clone(),
            true,
            context.to.path.clone(),
            &context,
            &OperateDirection::TO,
        ).expect("to_dict_info can not load");
        ttx.send(to_dict_info).unwrap();
    });
    (srx.recv().unwrap(), trx.recv().unwrap())
}

/// 对比两个文件的字节流，检查是否为同样的内容
/// from: https://users.rust-lang.org/t/efficient-way-of-checking-if-two-files-have-the-same-content/74735
fn is_same_file(f1: &File, f2: &File) -> bool {
    // Check if file sizes are different
    if f1.metadata().unwrap().len() != f2.metadata().unwrap().len() {
        return false;
    }

    // Use buf readers since they are much faster
    let f1 = BufReader::new(f1);
    let f2 = BufReader::new(f2);

    // Do a byte to byte comparison of the two files
    for (b1, b2) in f1.bytes().zip(f2.bytes()) {
        if b1.unwrap() != b2.unwrap() {
            return false;
        }
    }

    return true;
}

/// 询问是否继续
fn check_continue(hint: &str) {
    println!("{} [Y/N]", hint);
    let mut line = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut line).unwrap();
    if !line.to_uppercase().contains("Y") {
        exit(0);
    }
}

/// 预备结束
fn ready_to_exit() {
    println!("按下回车键结束……");
    let mut buf = [0];
    let stdin = io::stdin();
    stdin.lock().read(&mut buf).unwrap();
    exit(0);
}

fn copy_recursively(src: impl AsRef<Path>, dst: impl AsRef<Path>, overwrite: bool) -> Result<()> {
    if src.as_ref().is_file() {
        if dst.as_ref().exists() && overwrite {
            fs::remove_file(&dst)?;
            fs::copy(&src, &dst)?;
            copy_time(&src, &dst)?;
        } else if !dst.as_ref().exists() {
            fs::copy(&src, &dst)?;
            // 复制时间
            copy_time(&src, &dst)?;
        }
    } else {
        if !dst.as_ref().exists() {
            fs::create_dir(&dst)?;
            // 文件夹的试过了修改不了时间
        }
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
            } else {
                copy_recursively(entry.path(), dst.as_ref().join(entry.file_name()), overwrite)?;
            }
        }
    }

    Ok(())
}

fn copy_time(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
    let metadata = fs::metadata(src.as_ref()).unwrap();
    filetime::set_file_times(
        dst.as_ref(),
        FileTime::from_last_access_time(&metadata),
        FileTime::from_last_modification_time(&metadata),
    )?;

    Ok(())
}

#[cfg(target_os = "windows")]
fn adjust_canonicalization(p: String) -> String {
    const VERBATIM_PREFIX: &str = r#"\\?\"#;
    if p.starts_with(VERBATIM_PREFIX) {
        p[VERBATIM_PREFIX.len()..].to_string()
    } else {
        p
    }
}
