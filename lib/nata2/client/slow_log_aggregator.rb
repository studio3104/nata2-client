require 'nata2/client'
require 'net/ssh'
require 'mysql2-cs-bind'

module Nata2
  class Client
    class SlowLogAggregator
      def initialize(hostname, connection_settings)
        @hostname = hostname
        @config = connection_settings
      end

      def close_connections
        ssh_client.close
        mysql_client.close
      end

      def log_file_path
        show_variables('slow_query_log_file')
      end

      def log_file_inode
        result = ssh_exec("ls -i #{log_file_path}")
        result.empty? ? nil : result.split(' ').first.to_i
      end

      def log_file_lines
        result = ssh_exec("wc -l #{log_file_path}")
        result.empty? ? nil : result.split(' ').first.to_i
      end

      def raw_log_body(start_lines, fetch_lines)
        ssh_exec("sed -n '#{start_lines},#{start_lines + fetch_lines}p' #{log_file_path}")
      end

      def last_db(lines_previously)
        result = ssh_exec("sed -n '1,#{lines_previously}p' #{log_file_path} | egrep '^use|Schema:'")
        return nil unless result

        result = result.split("\n").last
        if m = result.match(/^use (\w+);/)
          return m[1]
        end
        if m = result.match(/Schema: (\w+)/)
          return m[1]
        end
      end

      def long_query_time
        show_variables('long_query_time')
      end

      private

      def validate_sql_components(*components)
        invalids = components.flatten.select { |c| !c.match(/\A[0-9a-zA-Z_\-]+\z/) }
        raise Error, "invalid bind variable - #{invalids}" unless invalids.empty?
        true
      end

      def show_variables(variable)
        return @variables[variable] if @variables.is_a?(Hash)

        @variables = {}

        mysql_exec('SHOW GLOBAL VARIABLES').each do |row|
          value = row['Value']
          value = case value
                  when /^\d+$/
                    value.to_i
                  when /^\d+\.\d+$/
                    value.to_f
                  else
                    value
                  end

          @variables[row['Variable_name']] = value
        end

        @variables[variable]
      end

      def mysql_exec(query, *bind_variables)
        if bind_variables.empty?
          mysql_client.xquery(query)
        else
          validate_sql_components(*bind_variables)
          mysql_client.xquery(query, bind_variables)
        end
      end

      def mysql_config
        @config[:mysql]
      end

      def mysql_client
        if @mysql_client && @mysql_client.ping
          @mysql_client
        else
          begin
            @mysql_client = Mysql2::Client.new(
              host: @hostname,
              port: mysql_config[:port],
              username: mysql_config[:username],
              password: mysql_config[:password],
            )
          rescue Mysql2::Error => e
            raise Error, e.message
          end
        end
      end

      def ssh_exec(command)
        result = {}
        ssh_client.exec(command) do |channel, stream, data|
          result[:stdout] = data if stream == :stdout
          result[:stderr] = data if data != '' && stream == :stderr
        end
        ssh_client.loop

        raise Error, "#{command} #{result[:stderr]}" if result[:stderr]
        result[:stdout]
      end

      def ssh_config
        conf = @config[:ssh]
        options = {}
        conf.each do |title, value|
          next if title == :username
          options[title] = value
        end
        {
          username: conf[:username],
          options: options
        }
      end

      def ssh_client
        if @ssh_client && !@ssh_client.closed?
          @ssh_client
        else
          @ssh_client = Net::SSH.start(@hostname, ssh_config[:username], ssh_config[:options])
        end
      end
    end
  end
end
