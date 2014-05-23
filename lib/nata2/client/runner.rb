require 'sqlite3'
require 'active_support/core_ext'
require 'nata2/client'
require 'nata2/client/db'
require 'nata2/client/slow_log_aggregator'
require 'nata2/client/parser'
require 'nata2/client/config'

class Nata2::Client
  class Runner
    attr_reader :hostname, :slowquery
    def initialize(hostname)
      @hostname = hostname
      @slowquery = SlowLogAggregator.new(hostname, Config.get(:connection_settings, hostname))
    end

    def run
      require 'awesome_print'
      # 前回実行時のファイルステータスと、現在のファイルステータスを取得
      current_status = {
        inode: slowquery.log_file_inode,
        lines: slowquery.log_file_lines,
      }

      require 'awesome_print'
      last_status = find_or_create_file_status(current_status)

      # 前回と現在の inode と処理済み行数が変わらなければ何もしない
      return if current_status == last_status.select { |k,v| k == :inode || k == :lines }
      ap current_status
      ap last_status.select { |k,v| k == :inode || k == :lines }

      start_lines = determine_fetch_start_lines(current_status, last_status)

      # 前回実行時に最後にどのデータベースでのスロークエリを処理したのか記録されてなかったら
      # start_lines より前の行から use 節か Schema から探してくる
      last_db = last_status[:last_db] ? last_status[:last_db] : slowquery.last_db(start_lines)

      raw_slow_logs = slowquery.raw_log_body(start_lines, Config.get(:fetch_lines_limit) - 1)
      long_query_time = slowquery.long_query_time
      slowquery.close_connections

      process(last_db, raw_slow_logs, long_query_time, start_lines - 1)
    end

    private

    def determine_fetch_start_lines(current_status, last_status)
      # inode が変わってなければ前回の続きから、変わってれば最初から取得する
      # 前回取得から inode が変わるまでに出力されたログは取って来られない...
      if current_status[:inode] == last_status[:inode]
        last_status[:lines] + 1
      else
        sqlite.execute(
          'UPDATE `file_status` SET `inode` = ?, `lines` = ? WHERE `hostname` = ?',
          current_status[:inode], 0, hostname
        )
        1
      end
    end

    # 生ログを意味のある単位に分割、パーズし、nata2 へ POST する
    def process(last_db, raw_slow_logs, long_query_time, processed_lines)
      sqlite.transaction
      begin
        Parser.split_raw_slow_logs(raw_slow_logs).each do |raw_slow_log|
          processed_lines = processed_lines + raw_slow_log.size
          parsed_slow_log = Parser.parse_slow_log(raw_slow_log)

          # どのデータベースへのクエリだったのかを明示
          if !parsed_slow_log[:db]
            if parsed_slow_log[:schema]
              parsed_slow_log[:db] = parsed_slow_log[:schema]
            else
              parsed_slow_log[:db] = last_db
            end
          end
          last_db = parsed_slow_log[:db]

          ap parsed_slow_log.merge(long_query_time: long_query_time)
ap          processed_lines
#          post_nata2(parsed_slow_log.merge(long_query_time: long_query_time))

          # 例外が起きた場合に、続きを次回実行時に順延させるために処理済みの行数を更新しながら行う
          sqlite.execute(
            'UPDATE `file_status` SET `lines` = ?, `last_db` = ?, `updated_at` = ? WHERE `hostname` = ?',
            processed_lines, last_db, Time.now.to_i, hostname
          )
        end
      #rescue #!!atode!! 例外クラス指定する
        #logger.error
      ensure
        sqlite.commit
        sqlite.close
      end
    end

    def find_or_create_file_status(current_status)
      last_status = sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', hostname).first

      # 前回情報がなかったら新規のホストとみなし、現在のファイルステータスを登録
      last_status = unless last_status.blank?
                      last_status
                    else
                      sqlite.execute(
                        'INSERT INTO `file_status` (`hostname`, `lines`, `inode`) VALUES (?, ?, ?)',
                        hostname,
                        current_status[:lines],
                        current_status[:inode],
                      )

                      sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', hostname).first
                    end

      last_status.symbolize_keys
    end

    def sqlite
      @sqlite3 ||= Nata2::Client::DB.new.client
    end

    def logger
      @logger ||= Logger.new()
    end

    def post_nata2(slowlog)
      # 200 じゃなかったら例外?
      # 400 だったらスキップ?
      # とか 200 以外のときにどうするかちゃんと決めて実装する
    end
  end
end