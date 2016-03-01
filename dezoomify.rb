#!/usr/bin/env ruby
# Dezoomify. See README.markdown.
# By Henrik Nyh <http://henrik.nyh.se> 2009-02-06 under the MIT License.
# Original code here: https://gist.github.com/henrik/59636

# requirements:
# brew install ghostscript
# brew install imagemagick

require 'open-uri'
require 'nokogiri'
require 'thread'
require 'fileutils'
require File.join(File.dirname(__FILE__), 'thread_pool')

TEMP_DIRECTORY = "/tmp"
TILES_DIRECTORY = "#{TEMP_DIRECTORY}/tiles"
ZOOMIFY_URL_PATTERN = "http://www.bach-digital.de/servlets/MCRZipFileNodeServlet/%s"

require 'net/http/persistent'
$http_connections = {}
def dz_http_conn
  $http_connections[Thread.current] ||= 
    Net::HTTP::Persistent.new('crawl').tap do |h|
      h.read_timeout = 30
      h.open_timeout = 30
    end
end

def dzopen_url(url)
  r = dz_http_conn.request URI(url)
  # r = HTTParty.get(url, :timeout => 60)
  r.body
rescue Timeout::Error
  puts "timeout while getting #{url}"
  ''
end

def dzdownload_url(url)
  data = nil
  retries = 0
  while !data
    data = dzopen_url(url) rescue nil
    retries += 1
    if retries > 10
      puts "Failed to download #{url}"
      raise "Failed to download #{url}"
    end
  end
  data
end

def qualify_fn(fn)
  fn = "#{TEMP_DIRECTORY}/#{fn.gsub('_', '/')}"
  FileUtils.mkdir_p(File.dirname(fn))
  fn
end

$dezoomify_pool = ThreadPool.new(10)

def dezoomify_jpg(url)
  return unless url =~ /MCRDFGServlet\/[^\/]+\/(.+)\.zip$/
  url = ZOOMIFY_URL_PATTERN % $1
  
  basename = File.basename(url)
  jpg_filename = qualify_fn("#{basename}.jpg")

  if File.file?(jpg_filename)
    STDOUT << '.'
    return IO.read(jpg_filename)
  end
  
  xml_url = "#{url}/ImageProperties.xml"
  doc = Nokogiri::XML(dzopen_url(xml_url))
  props = doc.at('IMAGE_PROPERTIES')
  
  width = props[:WIDTH].to_i
  height = props[:HEIGHT].to_i
  tilesize = props[:TILESIZE].to_f

  tiles_wide = (width/tilesize).ceil
  tiles_high = (height/tilesize).ceil

  # Determine max zoom level.
  # Also determine tile_counts per zoom level, used to determine tile group.
  # With thanks to http://trac.openlayers.org/attachment/ticket/1285/zoomify.patch.
  zoom = 0
  w = width
  h = height
  tile_counts = []
  while w > tilesize || h > tilesize
    zoom += 1

    t_wide = (w / tilesize).ceil
    t_high = (h / tilesize).ceil
    tile_counts.unshift t_wide*t_high

    w = (w / 2.0).floor
    h = (h / 2.0).floor
  end
  tile_counts.unshift 1  # Zoom level 0 has a single tile.
  tile_count_before_level = tile_counts[0..-2].inject(0) {|sum, num| sum + num }
  
  files = []
  
  tiles_high.times do |y|
    tiles_wide.times do |x|
      tile_url = ""
      tilename = '%s-%s-%s.jpg' % [zoom, x, y]
      local_filepath = qualify_fn("tiles/#{basename}/#{tilename}")
      files << local_filepath

      $dezoomify_pool.process do
        tile_group = ((x + y * tiles_wide + tile_count_before_level) / tilesize).floor

        tile_url = "#{url}/TileGroup#{tile_group}/#{tilename}"
        File.open(local_filepath, 'wb') {|f| f << dzdownload_url(tile_url) }
      end
    end
  end
  $dezoomify_pool.join

  # `montage` is ImageMagick.
  # We first stitch together the tiles of each row, then stitch all rows.
  # Stitching the full image all at once can get extremely inefficient for large images.

  STDOUT << "."

  `montage #{files.join(' ')} -geometry +0+0 -tile #{tiles_wide}x#{tiles_high} #{jpg_filename} 2>/dev/null`
  
  # delete tiles
  FileUtils.rm_rf("#{TEMP_DIRECTORY}/tiles/#{basename.gsub('_', '/')}") rescue nil
  

  IO.read(jpg_filename)
rescue => e
  puts e.message
end
