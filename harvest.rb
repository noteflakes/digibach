require 'rubygems'
require 'prawn'
require 'fileutils'
require 'nokogiri'
require 'httparty'
require 'open-uri'
require 'pp'

Prawn::Font::AFM.hide_m17n_warning = true

require File.expand_path(File.dirname(__FILE__), 'dezoomify')

TARGET_DIR = File.expand_path("~/digibach")
FileUtils.mkdir_p(TARGET_DIR) rescue nil

DERIVATE_URL_PATTERN = "http://www.bach-digital.de/servlets/MCRMETSServlet/%s?XSL.Style=dfg-source"

require 'net/http/persistent'
$http_connections = {}
def http_conn
  $http_connections[Thread.current] ||= 
    Net::HTTP::Persistent.new('crawl').tap do |h|
      h.read_timeout = 30
      h.open_timeout = 30
    end
end

def open_url(url)
  r = http_conn.request URI(url)
  # r = HTTParty.get(url, :timeout => 60)
  r.body
rescue Timeout::Error
  puts "timeout while getting #{url}"
  ''
end

def open_xml(url)
  Nokogiri::XML(open_url(url))
end

class String
  def uri_escape
    gsub(/([^ a-zA-Z0-9_.-]+)/n) {'%'+$1.unpack('H2'*$1.size).
      join('%').upcase}.tr(' ', '+')
  end
  
  def uri_unescape
    tr('+', ' ').gsub(/((?:%[0-9a-fA-F]{2})+)/n){[$1.delete('%')].pack('H*')}
  end
  
  def safe_uri_escape
    gsub(/([^\/]+)$/) {$1.uri_escape}
  end
  
  def safe_dir
    gsub(/\s/, '').gsub(/[:\/]/, '-')
  end

  def safe_fn
    gsub(/[:\/]/, '-')
  end
end

class Harvester
  def initialize(work, url, bwvs)
    @work = work
    @url = url
    @nice_url = (@url =~ /^(.+)\?/) && $1
    @bwvs = bwvs
    @h = open_xml(@url)
    @h.remove_namespaces!
  end
  
  def derivate_links
    @derivate_links ||= (@h/:derobject).select {|n| n['title'] =~ /zoomify/}.map do |n| 
      DERIVATE_URL_PATTERN % n['href']
    end
  end
  
  def derivate_docs
    @derivate_docs ||= derivate_links.map do |l|
      begin
        doc = open_xml(l)
        doc.remove_namespaces!
        doc
      rescue
        nil
      end
    end.compact
  end
  
  def title
    @title ||= (@h/:mcrstructure)[0]['classmark']
  end
  
  def metadata
    unless @meta
      @meta = {}
      @meta['work'] = @work
      @meta['bwvs'] = @bwvs
      @meta['url'] = @url
      @meta['title'] = (@h/:mcrstructure)[0]['classmark']
    end
    @meta
  end
  
  def derivate_info
    derivate_links.inject([]) do |m, l|
      begin
        # m << [l, derivate_jpg_hrefs(Hpricot(open_url(l)))]
        m << l
      rescue
      end
      m
    end
  end
  
  # Creates a list of jpgs from the derivate docs (a derivate doc is provided for each movement in the work)
  def jpg_hrefs
    last_label = ''
    last_jpg_url = ''
    doc_counter = -1
    
    @jpg_hrefs ||= derivate_docs.inject([]) do |jpgs, doc|
      doc_counter += 1 # keep counter in sync

      # Labels do not seem to appear in the zoomify XML files, so they'll be nil
      labels = (doc/'structMap[@TYPE="PHYSICAL"]'/'div[@TYPE="page"]').map do |n|
        n['ORDERLABEL']
      end
      
      urls = (doc/'fileGrp[@USE="MAX"]'/'FLocat').map do |n|
        n['href']
      end
      
      urls.each do |u|
        if File.basename(u) != File.basename(last_jpg_url)
          jpgs << [labels.shift, u]
          last_jpg_url = u
        end
      end

      jpgs
    end
  end
  
  def save_info
    info = metadata.merge('hrefs' => derivate_info)
    File.open(File.join(TARGET_DIR, "#{title.safe_fn}.yml"), 'w+') {|f| f << info.to_yaml}
  end
  
  def process_jpgs
    jpg_hrefs.each do |page_info|
      label = page_info.shift
      # Different refs for the same page may exist in different derivate docs,
      # here we just grab the first one that works and yield it to the 
      # supplied block.
      jpg = page_info.inject(nil) do |i, href|
        (jpg = dezoomify_jpg(href)) && (break jpg) rescue nil
      end
      yield jpg, label, page_info
    end
    # STDOUT << "\n"
  end
  
  def pdf_filename
    "#{TARGET_DIR}/#{title.safe_fn}.pdf"
  end
  
  def make_pdf
    return if derivate_docs.empty?
    
    pdf = Prawn::Document.new(:page_size => 'A4'); first = true
    pdf.font "Helvetica"
    pdf.font_size = 9
    page = 0
    process_jpgs do |jpg, label, hrefs|
      pdf.start_new_page unless first; first = false;
      page += 1
      # page identification
      pdf.text "page #{page}", :align => :center
      pdf.text "#{@work} - <link href='#{@nice_url}'><u>#{title}</u></link>", 
        inline_format: true, font_size: 7, align: :center
      if jpg
        begin
          pdf.image StringIO.new(jpg), :position => :center, :vposition => :center, :fit => [520, 700]
        rescue
          pdf.text "", :font_size => 20, :align => :left
          text = hrefs.inject("Failed to load jpg for #{label}:\n") do |t, r|
            t << "  #{r}\n"
          end
          pdf.text text, :font_size => 9, :align => :left
        end
      else
        puts "could not load jpg for #{label}"
        text = hrefs.inject("Could not load jpg for #{label}:\n") do |t, r|
          t << "  #{r}\n"
        end
        pdf.text "", :font_size => 20, :align => :left
        pdf.text text, :font_size => 14, :align => :left
      end
    end
    pdf.render_file(pdf_filename)
    puts
  end
  
  def self.get_receipts
    if File.file?(File.join(TARGET_DIR, "receipts.yml"))
      YAML.load(IO.read(File.join(TARGET_DIR, "receipts.yml")))
    else
      {}
    end
  end
  
  def self.set_receipts(r)
    File.open(File.join(TARGET_DIR, "receipts.yml"), 'w+') {|f| f << r.to_yaml}
  end
  
  def self.already_processed?(href, name)
    Thread.exclusive do
      get_receipts[href] && File.file?("#{TARGET_DIR}/#{name.safe_fn}.pdf")
    end
  end
  
  def self.record_receipt(href)
    Thread.exclusive do
      set_receipts(get_receipts.merge(href => true))
    end
  end
  
  def self.format_bwv_dir_name(bwv)
    case bwv
    when /^(\d+)(.*)$/
      "BWV%04d%s" % [$1.to_i, $2]
    when /^Anh/
      "BWV#{bwv}"
    else
      bwv
    end
  end
  
  def self.process(entry)
    bwvs = entry.map {|i| i['BWV']}.uniq
    href = entry[0]['href']
    name = entry[0]['name']

    if bwvs.size > 1
      range = "%s-%s" % [format_bwv_dir_name(bwvs[0]).safe_dir, format_bwv_dir_name(bwvs[-1]).safe_dir]
    else
      entry = entry[0]
      work = format_bwv_dir_name(entry['BWV'])
    end
    
    FileUtils.mkdir(TARGET_DIR) rescue nil

    unless already_processed?(href, name)
      m = new(work, href, bwvs)
      m.save_info
      m.make_pdf
      record_receipt(href)
    end
    #end
  rescue => e
    puts "Failed to process source for #{work}: #{e.message}"
    e.backtrace.each {|l| puts l}
  end
end

trap('INT') {exit}
trap('TERM') {exit}

$sources = YAML.load(IO.read('sources.yml'))
manuscripts = $sources.inject({}) do |m, w|
  href = w['href']
  (m[href] ||= []) << w
  m
end

idx = 1

manuscripts.each do |h, m|
  works = m.map {|i| Harvester.format_bwv_dir_name(i['BWV'])}.join(',')
  puts "(#{idx}) processing #{works}: #{m.first['name']}"
  Harvester.process(m)
  idx += 1
end

