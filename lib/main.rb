require 'sqlite3'
require 'csv'
require 'net/https'
require 'net/http'
require 'uri'
require 'open-uri'

@db = SQLite3::Database.open( "data/SOTD.db" )
@db.execute( "delete from csvRaw")

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
	val =""
	(0...5000).step(1000) do |skip|
		number = 0
		CSV.parse( get(skip, date), headers: true ) do |row|
			number += 1
			@db.execute "insert into csvRaw values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row.fields
			val = row[2]
		end
		puts number
	end
	return val
end


def getStateAbbr(state)

	hashFullName = { "alabama" => "AL",  "alaska" => "AK",   "arizona" => "AZ",   "arkansas" => "AR",   "california" => "CA",   "colorado" => "CO",   "connecticut" => "CT",   "delaware" => "DE",   "district of columbia" => "DC",   "florida" => "FL",   "georgia" => "GA",   "hawaii" => "HI",   "idaho" => "ID",   "illinois" => "IL",   "indiana" => "IN",   "iowa" => "IA",   "kansas" => "KS",   "kentucky" => "KY",   "louisiana" => "LA",   "maine" => "ME",   "maryland" => "MD",   "massachusetts" => "MA",   "michigan" => "MI",   "minnesota" => "MN",   "mississippi" => "MS",   "missouri" => "MO",   "montana" => "MT",   "nebraska" => "NE",   "nevada" => "NV",   "new hampshire" => "NH",   "new jersey" => "NJ",   "new mexico" => "NM",   "new york" => "NY",   "north carolina" => "NC",   "north dakota" => "ND",   "ohio" => "OH",   "oklahoma" => "OK",   "oregon" => "OR",   "pennsylvania" => "PA",   "rhode island" => "RI",   "south carolina" => "SC",   "south dakota" => "SD",   "tennessee" => "TN",   "texas" => "TX",   "utah" => "UT",   "vermont" => "VT",   "virginia" => "VA",   "washington" => "WA",   "west virginia" => "WV",   "wisconsin" => "WI",   "wyoming" => "WY" }

	state.strip!
	state = state.downcase
	if state.length == 2 then
		state = state.upcase
		return state
	end

	return hashFullName[state]
end

report = Array.new(3)
report[0] = "2014-03-10"
report[1] = loadAllForDate(report[0])
report[2] = "campaign"
report[3] = "false"

@db.execute " insert into reportInfo values (?, ?, ?, ?) ", report

report2 = Array.new(3)
report2[0] = "2014-03-11"
report2[1] =loadAllForDate(report2[0])
report2[2] = "campaign"
report2[3] = "false"


@db.execute "	insert into reportInfo values (?, ?, ?, ?) ", report2

@db.execute <<-SQL
	update csvraw
	set campaignID = campaignName
SQL

@db.execute <<-SQL
	update csvraw
	set deviceModel = "4"
	where deviceModel = "iPhone3,1"
	or deviceModel = "iPhone3,2"
	or deviceModel = "iPhone3,3"
	or deviceModel = "iPhone4,1"
SQL

@db.execute <<-SQL
	update csvraw
	set deviceModel = "5"
	where deviceModel = "iPhone5,1"
	or deviceModel = "iPhone5,2"
	or deviceModel = "iPhone6,1"
SQL

@db.execute <<-SQL
	update csvraw
	set deviceModel = "5c"
	where deviceModel = "iPhone5,3"
SQL

@db.execute <<-SQL
	update csvraw
	set campaignID = "Both"
	where uuid in
		(select uuid
		from (
			select a.uuid, count(*)
			from
				(select uuid, campaignName
				from csvraw
				group by uuid, campaignName) a
			group by a.uuid
			having count(*) > 1)
		)
SQL

@db.execute <<-SQL
	drop table adjusted
SQL

@db.execute <<-SQL
	create table adjusted AS
	select *
	from csvraw
	group by uuid, campaignID
SQL

@db.execute <<-SQL
	delete from mailingaddressinfo
SQL

@db.execute( " select fullName, address, uuid, deviceModel, campaignID, partNumber from adjusted " ) do |row|
	insertRow = Array.new(10)
	insertRow[0] = row[0]

	address = row[1].split(",")
	street = ""
	city = ""
	state = ""
	zip = ""
	country = ""

	if address.length == 6 then
		street = "#{address[0]} #{address[1]}"
		city = address[2]
		state = address[3]
		zip = address[5]
		country = address[4]
	else
		street = address[0]
		city = address[1]
		state = address[2]
		zip = address[4]
		country = address[3]
	end

	insertRow[1] = street
	insertRow[2] = city
	insertRow[3] = getStateAbbr(state)
	insertRow[4] = country
	insertRow[5] = zip
	insertRow[6] = row[3]
	insertRow[7] = row[4]

	row9 = "#{row[5].to_s[4...8]}#{row[4].to_s[0...5]}#{row[3]}"

	insertRow[8] = row9
	insertRow[9] = row[2]


	@db.execute "insert into mailingaddressinfo values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" , insertRow

end


report3 = Array.new(3)
report3[0] = Date.today.to_s
report3[1] = report[1] << " & " << report2[1]
report3[2] = "mail"
report3[3] = "false"


@db.execute "	insert into reportInfo values (?, ?, ?, ?) ", report3

@db.close