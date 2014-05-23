require 'spec_helper'
require 'nata2/client/slow_log_aggregator'

describe Nata2::Client::SlowLogAggregator do
  let(:hostname) { 'test.host' }
  let(:connection_settings) { {} }
  let(:obj) { Nata2::Client::SlowLogAggregator.new(hostname, connection_settings) }

  describe '#log_file_path' do
  end

  describe '#log_file_inode' do
    before do
      allow(obj).to receive(:log_file_path) {}
      allow(obj).to receive(:ssh_exec) {command_result}
    end
    context 'blank result' do
      let(:command_result) { '' }
      it { expect(obj.log_file_inode).to eq(nil) }
    end
    context 'result' do
      let(:command_result) { '707459 slow.log' }
      it { expect(obj.log_file_inode).to eq(707459) }
    end
  end

  describe '#log_file_lines' do
    before do
      allow(obj).to receive(:log_file_path) {}
      allow(obj).to receive(:ssh_exec) {command_result}
    end
    context 'blank result' do
      let(:command_result) { '' }
      it { expect(obj.log_file_lines).to eq(nil) }
    end
    context 'result' do
      let(:command_result) { '564 slow.log' }
      it { expect(obj.log_file_lines).to eq(564) }
    end
  end

  describe '#log_body' do
  end

  describe '#last_db' do
    before do
      allow(obj).to receive(:log_file_path) {}
      allow(obj).to receive(:lines_previously) {}
      allow(obj).to receive(:ssh_exec) {command_result}
    end
    context 'blank result' do
      let(:command_result) { nil }
      it { expect(obj.last_db(0)).to eq(nil) }
    end
    context 'single line result (use)' do
      let(:command_result) { "use sbtest;" }
      it { expect(obj.last_db(0)).to eq('sbtest') }
    end
    context 'single line result (Schema)' do
      let(:command_result) { "# Thread_id: 41  Schema: sbtest  Last_errno: 0  Killed: 0" }
      it { expect(obj.last_db(0)).to eq('sbtest') }
    end
    context 'multiple lines results' do
      let(:command_result) { "# Thread_id: 41  Schema: sbtest  Last_errno: 0  Killed: 0\nuse sbtest;" }
      it { expect(obj.last_db(0)).to eq('sbtest') }
    end
  end

  describe '#long_query_time' do
  end

  describe '#validate_sql_components' do
    let(:valid_sql_component) { 'mysql' }
    let(:invalid_sql_component) { '; DROP DATABASE `mysql`;'}

    context 'valid pattern' do
      it do
        expect(obj.send(:validate_sql_components, valid_sql_component)).to eq(true)
      end
    end

    context 'invalid pattern' do
      it do
        expect {
          obj.send(:validate_sql_components, invalid_sql_component)
        }.to(
          raise_error(Nata2::Client::Error)
        )
      end

      it do
        expect {
          obj.send(
            :validate_sql_components,
            valid_sql_component,
            invalid_sql_component
          )
        }.to(
          raise_error(Nata2::Client::Error)
        )
      end
    end
  end
end
