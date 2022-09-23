use std::rc::Rc;
use std::str::FromStr;
use clap::Parser;
use config::{Config, Value};

fn main() {
    let args = Args::parse();
    let context = read_config(&args.file);

    println!("context: {:#?}", context);
}

#[derive(Parser, Debug)]
#[clap(version, about = "简单的本地文件同步", long_about = None)]
struct Args {
    /// 配置文件路径
    #[clap(default_value_t = String::from("ssync.yml"), short, long, value_parser)]
    file: String,
}

#[derive(Debug)]
struct SyncPath {
    /// 目录路径
    path: String,
    /// 白名单正则
    include: Vec<String>,
    /// 排除正则
    exclude: Vec<String>,
}

#[derive(Debug)]
struct SyncContext {
    /// 被同步目录信息
    from: SyncPath,
    /// 目标目录信息
    to: SyncPath,
    /// 是否递归子文件夹
    recursive: bool,
}

/// 读取配置文件
fn read_config(file_path: &str) -> SyncContext {
    let settings = Config::builder()
        .add_source(config::File::with_name(file_path))
        .build()
        .unwrap();

    let mut from_settings = settings.get_table("from").unwrap();
    let mut to_settings = settings.get_table("to").unwrap();

    return SyncContext {
        from: SyncPath {
            path: from_settings.remove("path").unwrap().into_string().unwrap(),
            include: to_string_vec(from_settings.remove("include")),
            exclude: to_string_vec(from_settings.remove("exclude")),
        },
        to: SyncPath {
            path: to_settings.remove("path").unwrap().into_string().unwrap(),
            include: to_string_vec(to_settings.remove("include")),
            exclude: to_string_vec(to_settings.remove("exclude")),
        },
        recursive: settings.get_bool("recursive").unwrap_or(false),
    };
}

fn to_string_vec(value_vec: Option<Value>) -> Vec<String> {
    match value_vec {
        Some(value) => {
            let mut vec = Vec::new();
            for v in value.into_array().unwrap() {
                vec.push(v.into_string().unwrap())
            }
            vec
        }
        None => vec![]
    }
}
