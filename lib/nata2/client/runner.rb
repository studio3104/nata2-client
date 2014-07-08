require 'sqlite3'
require 'net/http'
require 'uri'
require 'json'
require 'active_support/core_ext'
require 'mysql-slowquery-parser'
require 'nata2/client'
require 'nata2/client/db'
require 'nata2/client/slow_log_aggregator'
require 'nata2/client/config'

class Nata2::Client
  class Runner
    def initialize(servicename, hostname)
      @servicename = servicename
      @hostname = hostname
      @slowquery = SlowLogAggregator.new(hostname, Config.get(:connection_settings, hostname))
    end

    def run
      # 前回実行時のファイルステータスと、現在のファイルステータスを取得
      current_status = {
        inode: @slowquery.log_file_inode,
        lines: @slowquery.log_file_lines,
      }

      last_status = find_or_create_file_status(current_status)

      # 前回と現在の inode と処理済み行数が変わらなければ何もしない
      if current_status == last_status.select { |k,v| k == :inode || k == :lines }
        logger.info(@hostname) { %Q{No changed. #{JSON.generate(current_status)}} } 
        return
      end

      start_lines = determine_fetch_start_lines(current_status, last_status)

      raw_slow_logs = @slowquery.raw_log_body(start_lines, Config.get(:fetch_lines_limit) - 1)
      long_query_time = @slowquery.long_query_time
      @slowquery.close_connections

      process(last_status[:last_db], raw_slow_logs, long_query_time, start_lines - 1)
    rescue Nata2::Client::Error => e
      logger.error(@hostname) { %Q{#{e.message}} }
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
          current_status[:inode], 0, @hostname
        )
        1
      end
    end

    # 生ログを意味のある単位に分割、パーズし、nata2 へ POST する
    def process(last_db, raw_slow_logs, long_query_time, processed_lines)
      sqlite.transaction
      begin
        parser.split_raw_slow_logs(raw_slow_logs).each do |raw_slow_log|
          processed_lines = processed_lines + raw_slow_log.size
          parsed_slow_log = parser.parse_slow_log(raw_slow_log)

          # どのデータベースへのクエリだったのかを明示
          if !parsed_slow_log[:db]
            if parsed_slow_log[:schema]
              parsed_slow_log[:db] = parsed_slow_log[:schema]
            else
              # parse されたスロークエリログと、前回実行時の記録から、
              # 最後にどのデータベースでのスロークエリを処理したのかがわからない場合、
              # processed_lines より前の行から use 節か Schema から探してくる
              parsed_slow_log[:db] = last_db ? last_db : @slowquery.last_db(processed_lines)
            end
          end
          last_db = parsed_slow_log[:db]

          post_nata2(parsed_slow_log.merge(long_query_time: long_query_time))

          # 例外が起きた場合に、続きを次回実行時に順延させるために処理済みの行数を更新しながら行う
          sqlite.execute(
            'UPDATE `file_status` SET `lines` = ?, `last_db` = ?, `updated_at` = ? WHERE `hostname` = ?',
            processed_lines, last_db, Time.now.to_i, @hostname
          )
        end
      rescue => e
        logger.error(@hostname) { "#{e.message} (#{e.class.to_s})" }
      ensure
        sqlite.commit
        sqlite.close
      end
    end

    def find_or_create_file_status(current_status)
      last_status = sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', @hostname).first

      # 前回情報がなかったら新規のホストとみなし、現在のファイルステータスを登録
      last_status = unless last_status.blank?
                      last_status
                    else
                      sqlite.execute(
                        'INSERT INTO `file_status` (`hostname`, `lines`, `inode`) VALUES (?, ?, ?)',
                        @hostname,
                        current_status[:lines],
                        current_status[:inode],
                      )

                      sqlite.execute('SELECT * FROM `file_status` WHERE `hostname` = ?', @hostname).first
                    end

      last_status.symbolize_keys
    end

    def sqlite
      @sqlite3 ||= Nata2::Client::DB.new.client
    end

    def logger
      @logger ||= Logger.new(Dir.tmpdir + '/nata-client.log', 10)
    end

    def parser
      MySQLSlowQueryParser
    end

    def post_nata2(slowlog)
      nataserver = Config.get(:nataserver)
      databasename = slowlog.delete(:db)
      api = URI.parse(%Q{http://#{nataserver[:fqdn]}:#{nataserver[:port]}/api/1/#{@servicename}/#{@hostname}/#{databasename}})
      request = Net::HTTP::Post.new(api.path)
      request.set_form_data(slowlog)
      http = Net::HTTP.new(api.host, api.port)
      response = http.start.request(request)

      if !response
        raise Nata2::Client::Error, 'No response from Nata server'
      end

      response_code = response.code.to_i
      unless [200, 400].include?(response_code)
        raise Nata2::Client::Error, %Q{Nata server returns #{response_code}.}
      end

      response_body = JSON.parse(response.body)
      case response_body['error']
      when 0
        logger.info(@hostname) { %Q{Post successful. id: #{response_body['data']['id']}} }
      when 1
        # ここに該当する場合は、Nata Server 側の Validation で弾かれた場合なので、
        # 同じロジックで再度 Parse~ しても同じことになる。
        # そのため、例外を起こさず該当のスロークエリログはスキップするようにした。
        logger.error(@hostname) { %Q{Failed to post. messages: #{response_body['messages']}} }
      else
        raise Nata2::Client::Error, %Q{Unknown error status: #{response_body['error']}}
      end
    end
  end
end
