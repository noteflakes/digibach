require 'net/http/persistent'
require 'digest/sha1'
require 'fileutils'

module Cache
  class << self
    def key(v)
      Digest::SHA1.hexdigest(v)
    end
    
    CACHE_ROOT = "/tmp/digibach/cache"
    
    def key_to_path(key)
      part1 = key[0]
      part2 = key[1]
      part3 = key[2]
      part4 = key[3..-1]
      dir = "#{CACHE_ROOT}/#{part1}/#{part2}/#{part3}"
      FileUtils.mkdir_p(dir)
      "#{dir}/#{part4}"
    end
    
    def url_cache_path(url)
      key_to_path(key(url))
    end
    
    def cached_download(url)
      k = key(url)
      cached = get(k) { open_url(url) }
    end
    
    def cached_xml(url)
      xml = cached_download(url)
      if xml =~ /<html/
        del(key(url))
        o = Nokogiri::XML(xml).at("[class='dpt_error_message_trace']") rescue nil
        if o
          msg = o.content.strip
          puts "Error signaled downloading #{url}:"
          puts msg
        end
        raise
      end
      
      Nokogiri::XML(xml).tap {|d| d.remove_namespaces!}
    end
    
    def get(key, &block)
      path = key_to_path(key)
      if File.file?(path)
        IO.read(path)
      elsif block
        block.call.tap {|v| set(key, v)}
      else
        nil
      end
    end
    
    HTTP_CONNECTIONS = {}
    
    def http_conn
      HTTP_CONNECTIONS[Thread.current] ||= 
        Net::HTTP::Persistent.new('cache').tap do |h|
          h.read_timeout = 30
          h.open_timeout = 30
        end
    end
    
    def set(key, value)
      path = key_to_path(key)
      File.open(path, 'w') {|f| f << value }
    end
    
    def del(key)
      File.rm(key_to_path(key)) rescue nil
    end
    
    def open_url(url, retry_count = 0)
      r = http_conn.request URI(url)

      if ['301', '302'].include?(r.code)
        r.header['location'] =~ /^([^;]+)/
        new_url = BASE_URL + $1
        return open_url(new_url) 
      end

      r.body
    rescue Timeout::Error
      if retry_count < 3
        open_url(url, retry_count + 1)
      else
        puts "timeout while getting #{url}"
        ''
      end
    end
  end
end