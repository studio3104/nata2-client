require 'sqlite3'
require 'active_support/core_ext'
require 'nata2/client/slow_log_aggregator'
require 'nata2/client/config'

module Nata2
  class Client
    def run(hostname)
      #!!atode!! hostname の validation

      slowquery = SlowLogAggregator.new(hostname, Config.get(:connection_settings, hostname))

      # 前回実行時のファイルステータスと、現在のファイルステータスを取得
      current_status = {
        inode: slowquery.log_file_inode,
        lines: slowquery.log_file_lines,
      }
      last_status = find_or_create_file_status(hostname, current_status)

      # 前回と現在の inode と処理済み行数が変わらなければ何もしない
      return if current_status == last_status.select { |k,v| k == :inode || k == :lines }

      # 前回実行時に最後にどのデータベースでのスロークエリを処理したのか記録されてなかったら
      # start_lines より前の行から use 節か Schema から探してくる
      last_db = last_status[:last_db] ? last_status[:last_db] : slowquery.last_db(start_lines)

      start_lines = determine_fetch_start_lines(current_status, last_status)

      raw_slow_logs = slowquery.raw_log_body(start_lines, Config.get(:fetch_lines_limit))
      long_query_time = slowquery.long_query_time
      slowquery.close_connections

      process(hostname, last_db, raw_slow_logs, long_query_time, start_lines)
    end

    private

    def determine_fetch_start_lines(current_status, last_status)
      # inode が変わってなければ前回の続きから、変わってれば最初から取得する
      # 前回取得から inode が変わるまでに出力されたログは取って来られない...
      if current_status[:inode] == last_status[:inode]
        last_status[:lines]
      else
        sqlite.execute(
          'UPDATE `file_status` SET `inode` = ?, `lines` = ? WHERE `hostname` = ?',
          current_status[:inode], 0, hostname
        )
        0
      end
    end

    # 生ログを意味のある単位に分割、パーズし、nata2 へ POST する
    def process(hostname, last_db, raw_slow_logs, long_query_time, processed_lines)
      sqlite.transaction
      begin
        split_raw_slow_logs(raw_slow_logs).each do |raw_slow_log|
          processed_lines = processed_lines + raw_slow_log.split("\n").size
          parsed_slow_log = parse_slow_log(raw_slow_log)

          # どのデータベースへのクエリだったのかを明示
          if !parsed_slow_log[:db]
            if !parsed_slow_log[:schema]
              parsed_slow_log[:db] = parsed_slow_log[:schema]
            else
              parsed_slow_log[:db] = last_db
            end
          end
          last_db = parsed_slow_log[:db]

          post_nata2(parsed_slow_log.merge(long_query_time: long_query_time))

          # 例外が起きた場合に、続きを次回実行時に順延させるために処理済みの行数を更新しながら行う
          sqlite.execute(
            'UPDATE `file_status` SET `lines` = ?, `last_db` = ? WHERE `hostname` = ?',
            processed_lines, last_db, hostname
          )
        end
      rescue #!!atode!! 例外クラス指定する
        #logger.error
      ensure
        sqlite.commit
        sqlite.close
      end
    end

    def find_or_create_file_status(hostname, current_status)
      last_status = sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', hostname).first

      # 前回情報がなかったら新規のホストとみなし、現在のファイルステータスを登録
      last_status = if last_status.blank?
                      sqlite.execute(
                        'INSERT INTO `file_status` (`hostname`, `lines`, `inode`) VALUES (?, ?, ?)',
                        hostname,
                        current_status[:lines],
                        current_status[:inode],
                      )

                      sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', hostname)
                    end

      last_status.symbolize_keys
    end

    # 生スローログを意味のある単位に分割
    def split_raw_slow_logs(raw_slow_logs)
      result = []
      part = []

      raw_slow_logs.each_line do |line|
        part << line
        if line.end_with?(';', ";\n") && !line.start_with?('use ', 'SET timestamp=')
          result << part
          part = []
        end
      end

      result
    end

    def parse_slow_log(raw_slow_log)
      result = {}
      line = raw_slow_log.shift

      while !line.start_with?('#')
        # こういうのを suppress する
        #### /usr/local/Cellar/mysql/5.6.12/bin/mysqld, Version: 5.6.12 (Source distribution). started with:
        #### Tcp port: 3306  Unix socket: /tmp/mysql.sock
        line = raw_slow_log.shift
      end

      if record = line.match(/^# Time:\s+(.+)/)
        result[:time] = Time.parse(record[1]).to_i
        line = raw_slow_log.shift
      end

      if record = line.match(/^# User@Host: ([^\[]+)?\[([^\]]*)\] @ ([^\[]+)\[([^\]]*)\]/)
        result[:user] = record[1].blank? ? record[2] : record[1]
        result[:host] = record[3].blank? ? record[4] : record[3].strip
        line = raw_slow_log.shift
      end

      # こういうのをハッシュにする
      #### # Query_time: 4.267253  Lock_time: 0.000017  Rows_sent: 0  Rows_examined: 734266  Rows_affected: 734266  Rows_read: 734266
      while line.start_with?('#')
        line = line.sub(/^#\s+/, '')

        # '  ' で split したかったけど、' ' で区切られてる場合もあったからこうした
        record = line.split(/\s+/).map { |val|
          case val
          when /\:$/
            val.sub(/\:$/, '').downcase.to_sym
          when /^\d+$/
            val.to_i
          when /^\d+\.\d+$/
            val.to_f
          else
            val
          end
        }
        result = result.merge(Hash[*record])

        line = raw_slow_log.shift
      end

      if record = line.match(/^use (\w+);$/)
        result[:db] = record[1]
        line = raw_slow_log.shift
      end

      if record = line.match(/^SET timestamp=(\d+);$/)
        result[:time] = record[1].to_i
        line = raw_slow_log.shift
      end

      result[:sql] = line
      raw_slow_log.each do |l|
        result[:sql] = result[:sql] + l
      end
      result[:sql] = result[:sql].sub(/;$/, '')
      result[:sql] = result[:sql].strip

      result
    end

    def sqlite
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
