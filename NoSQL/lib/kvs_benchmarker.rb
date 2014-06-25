require 'handlersocket'
require 'mysql2'
require 'memcache'
require 'redis'

module KvsBenchmarker
  class Sample
    CSV_FILE = "/tmp/hs_benchmark.txt"

    attr_reader :samples, :size, :csv_filename
    def initialize(options = {})
      @size = options[:size]
      @samples = []
      1.upto(0/(0.0)) do |id|
        @samples << [ id, url ]
        break if id == size
      end 

      export_csv
      @csv_filename = CSV_FILE
    end

    def export_csv
      open(CSV_FILE, "w") do |f|
        @samples.each do |row|
          f.write row.join(',') + "\n"
        end
      end
    end

    def values
      @samples.each { |sample| sample.last }
    end

    def key_name
      'id'
    end

    def value_name
      'url'
    end

    def colomn_names
      [key_name, value_name]
    end

    private
    def url
      "http://" + rand( 36**(rand(10) + 10) ).to_s(36) + ".co.jp/" + rand( 36**(rand(10) + 10) ).to_s(36)
    end
  end

  class MySQLBench
    def initialize(options = {})
      @client = Mysql2::Client.new(
        :host      => options[:host],
        :port      => options[:port],
        :database  => options[:database],
        :username  => options[:username]
      )
      @table = options[:table]
      @sample= options[:sample]
      @sample_datas = options[:sample].samples
    end

    def drop_data
      @client.query("DROP TABLE IF exists #{@table}")
      @client.query("
create table #{@table} (
  id int AUTO_INCREMENT PRIMARY KEY,
  url varchar(100) NOT NULL
) ENGINE = innodb DEFAULT CHARSET UTF8;
      ")

    end

    def setup
      drop_data
      @client.query("LOAD DATA INFILE '#{@sample.csv_filename}' INTO TABLE #{@table} FIELDS TERMINATED BY ',';")
    end

    def get
      sql = "SELECT * FROM #{@table} WHERE #{@sample.key_name} = #{rand(@sample.size + 1)}"
      query(sql)
    end

    def set
      @sample_datas.each do |row|
        sql = "INSERT INTO #{@table} VALUES ( '', '#{row.last}')"
        query(sql)
      end
    end

    def close
      @client.close
    end

    private
    def query(sql)
      @client.query(sql, :as => :array, :cache_rows => false, :cast => false)
    end
  end

  class HandlerSocketBench
    def initialize(options = {})
      @hs = HandlerSocket.new( :host => options[:host], :port => options[:port] )
      @database = options[:database]
      @table    = options[:table]
      @sample   = options[:sample]
      @sample_datas = options[:sample].samples
      @index_id = rand(options[:sample].samples.size)
      @hs.open_index(@index_id, @database, @table, 'PRIMARY', "#{@sample.colomn_names.join(',')}") 
    end

    def get
      @hs.execute_single(@index_id, '=', [rand(@sample.size + 1)], 1, 0)
    end

    def get_first_record
      @hs.execute_single(@index_id, '=', [1], 1, 0)
    end

    def get_slowly
      @hs.execute_single(@index_id, '=', [rand(@sample.size + 1)], 1, 0)
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".split(//)
    end

    def get_first_record_slowly
      @hs.execute_single(@index_id, '=', [1], 1, 0)
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".split(//)
    end


    def get_multi
      condition = [ [@index_id, '=', [rand(@sample.size + 1)], 1, 0] ]
      @hs.execute_multi(condition)
    end

    def set
      @sample_datas.each do |row|
        @hs.execute_insert(@index_id, ["#{row.first}","#{row.last}"])
      end
    end

    def close
      @hs.close
    end
  end

  class RedisBench
    def initialize(options = {})
      @redis = Redis.new
      @sample_datas = options[:sample].samples
    end

    def drop_data
      @redis.flushall
    end

    def set
      @sample_datas.each do |row|
        @redis.set(row.first.to_s, row.last.to_s)
      end
    end

    def setup
      drop_data
      set
    end

    def get
      @redis.get(rand(@sample_datas.size + 1).to_s)
    end

    def close
      @redis.quit
    end
  end

  class MemcachedBench
    def new(options = {})
      @cache = MemCache.new(
        ["#{options[:host]}:#{options[:port]}"],
        { :timeout => 1 }
      )
      @sample_datas = Marshal.load(Marshal.dump(options[:sample].samples))
    end

    def drop_data
      @cache.flush_all
    end

    def setup
      drop_data
      @sample_datas.each do |row|
        @cache.set(row.shift.to_s, row.last.to_s)
      end
      @cache.reset
    end

    def get
      @cache.get(rand(@sample_datas.size + 1).to_s)
    end

    def close
      @cache.reset
    end
  end

end
