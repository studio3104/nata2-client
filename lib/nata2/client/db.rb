require 'nata2/client'
require 'sqlite3'

class Nata2::Client::DB
  attr_reader :client

  def initialize
    @client = SQLite3::Database.new(File.join(File.dirname(__FILE__), '../../../nata2client.db'))
    @client.results_as_hash = true
  end

  def init
    @client.transaction do
      @client.execute <<-SQL
        CREATE TABLE IF NOT EXISTS `file_status` (
          `id`         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
          `hostname`   VARCHAR(255),
          `lines`      INTEGER,
          `inode`      INTEGER,
          `last_db`    VARCHAR(255),
          `updated_at` INTEGER NOT NULL DEFAULT (STRFTIME('%s', 'now', 'localtime')),
          `created_at` INTEGER NOT NULL DEFAULT (STRFTIME('%s', 'now', 'localtime'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS `index_log_file_status_on_hostname` ON `log_file_status` (`hostname`);
      SQL
    end
  end
end
