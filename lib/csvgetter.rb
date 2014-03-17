require 'sqlite3'
require 'csv'
require 'net/https'
require 'net/http'
require 'uri'
require 'open-uri'

class csvgetter

	def initialize
		@db = SQLite3::Database.open( "data/SOTD.db" )
		@db.execute( "delete from csvRaw")
	end

	def get(skip, date)
		uritoparse = "https://data.skinofthedayapp.com/report?skip=#{skip}&campaigndate=#{date}&export=true&export%3Dtrue%26action=Export+to+CSV"
		uri = URI.parse(uritoparse)
		http = Net::HTTP.new(uri.host, uri.port)
		http.use_ssl = true
		http.verify_mode = OpenSSL::SSL::VERIFY_NONE
		request1 = Net::HTTP::Get.new(uri.request_uri)
		resp = http.request(request1)
		arrayb = resp.body.split("\n")
		href = arrayb[6]
		urlofcsv = href[17...-2]
		csv = open("#{urlofcsv}")

		return csv.read
	end


	def loadAllForDate(date)
		(0...5000).step(1000) do |skip|
			number = 0
			CSV.parse( get(skip, date), headers: true ) do |row|
				number += 1
				@db.execute "insert into csvRaw values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row.fields
			end
			puts number
		end
	end
end


