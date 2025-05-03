require "option_parser"
require "process"
require "sqlite3"
require "db"
require "yaml"

# check is file sqlite?
def sqlite_file?(file : String)
  result = false
  DB.open "sqlite3://./#{file}" do |db|
    count = db.scalar "select count() from sqlite_master where type='table'"
    result = true if count.as(Int) > 0
  end
  result
end

# check is file wiz database?
def wiz_db?(file : String)
  result = false
  DB.open "sqlite3://./#{file}" do |db|
    count = db.scalar "select count() from sqlite_master where name='wiz' AND type='table'"
    result = true if count.as(Int) == 1
  end
  result
end

# create repo schema in database
def create_schema
  DB.open "sqlite3://./#{Global.db}" do |db|
    db.exec "CREATE TABLE wiz(format_version TEXT)"
    db.exec "INSERT INTO wiz VALUES(?)", "1"

    db.exec "CREATE TABLE packages(\
    pkgId Integer PRIMARY KEY AUTOINCREMENT,\
    name TEXT NOT NULL UNIQUE,\
    version TEXT NOT NULL,\
    maintainer TEXT NOT NULL,\
    description TEXT,\
    is_installed BOOLEAN\
    )"

    db.exec "CREATE TABLE files(\
    fileId INTEGER PRIMARY KEY AUTOINCREMENT,\
    pkgId INTEGER NOT NULL,\
    path TEXT NOT NULL,\
    FOREIGN KEY (pkgId) REFERENCES packages(pkgId) ON DELETE CASCADE\
    )"

    db.exec "CREATE TABLE dependencies(\
    depId INTEGER PRIMARY KEY AUTOINCREMENT,\
    pkgId INTEGER NOT NULL,\
    depName TEXT NOT NULL,\
    FOREIGN KEY (pkgId) REFERENCES packages(pkgId) ON DELETE CASCADE\
    )"
  end
end

# add single tar file's metadata to database
def add_package(file : String)
  p = Process.new("bsdtar", ["-xf", file, "-O", "props.yml"], output: Process::Redirect::Pipe)
  props_str = p.output.gets_to_end
  p.wait

  props = YAML.parse(props_str)
  name = props["name"].to_s
  version = props["version"].to_s
  maintainer = props["maintainer"].to_s
  description = props["description"].to_s
  deps = props["deps"]

  # read files_list from tar
  p = Process.new("bsdtar", ["-xf", file, "-O", "files"], output: Process::Redirect::Pipe)
  files_str = p.output.gets_to_end
  p.wait

  files_list = files_str.split("\n")
  is_installed = 0
  DB.open "sqlite3://./#{Global.db}" do |db|
    db.exec("INSERT INTO packages VALUES (?, ?, ?, ?, ?, ?)",
      nil, # auto id
      name,
      version,
      maintainer,
      description,
      is_installed
    )

    pkg_id = db.scalar "select pkgId from packages where name='#{name}'"
    raise "pkg_id must be integer" if pkg_id.is_a?(String)

    files_list.each do |file_path|
      db.exec("INSERT INTO files VALUES (?, ? ,?)",
        nil, # auto id
        pkg_id,
        file_path
      )
    end

    deps.as_a.each do |dep_name|
      db.exec("INSERT INTO dependencies VALUES (?, ? ,?)",
        nil, # auto id
        pkg_id,
        dep_name.as_s
      )
    end
  end
end

# add tars metadata in a directory to database
def add_dir
  # Delete previous data from all tables
  DB.open "sqlite3://./#{Global.db}" do |db|
    db.exec("DELETE from packages")
    db.exec ("DELETE from sqlite_sequence where name='packages'")
    db.exec("DELETE from files")
    db.exec ("DELETE from sqlite_sequence where name='files'")
    db.exec("DELETE from dependencies")
    db.exec ("DELETE from sqlite_sequence where name='dependencies'")
  end

  pkg_list = Dir.glob("*.wpkg.tar.zstd")
  pkg_list.each do |pkg_name|
    puts "adding #{pkg_name} to repository"
    add_package(pkg_name)
  end
end

# Default name of db
class Global
  @@db = "index.sqlite3"
  def self.db
    @@db
  end
end

option_parser = OptionParser.parse do |parser|
  parser.banner = "Welcome to Repository Indexer for wiz packages"

  parser.on "-v", "--version", "Show version" do
    puts "version 1.0"
    exit
  end
  parser.on "-h", "--help", "Show help" do
    puts parser
    exit
  end
  parser.on "-a DIR|PACKAGE", "--add=DIR|PACKAGE", "Add directory or package to DB" do |dir|
    Dir.cd(dir)
    mFile = Global.db

    if File.exists?(mFile)
      if sqlite_file?(mFile)
        create_schema unless wiz_db?(mFile)
      else
        raise "#{mFile} is not a sqlite3 or wiz db file"
      end
    else
      File.touch(mFile)
      create_schema
    end

    add_dir
  end
end

option_parser

# DB.open "sqlite3://./data.db" do |db|
#   db.exec "create table contacts (name text, age integer) "
#   db.exec "insert into contacts values (?, ?)", "John Doe", 30
#
#   args = [] of DB::Any
#   args << "Sarah"
#   args << 33
#   db.exec "insert into contacts values (?, ?)", args: args
#
#   puts "max age:"
#   puts db.scalar "select max(age) from contacts"
#
#   puts "contacts:"
#   db.query "select name, age from contacts order by age desc" do |res|
#     puts "#{res.column_name(0)} (#{res.column_name(1)})"
#     res.each do
#       puts "#{res.read(String)} (#{res.read(Int32)})"
#     end
#   end
# end
