require 'sqlite3'
require 'csv'

db = SQLite3::Database.open( "data/SOTD.db" )

db.execute "delete from mailed"

CSV.parse( open( "data/mailed.csv " ), headers: true ) do |row|
	db.execute "insert into mailed values (?)", row.fields
end


rows = db.execute <<-SQL
	select * from reportinfo where reported = "false"
SQL

rows.each do |row|

	date = row[0]
	campaignName = row[1]
	reportName = "R-#{date}-#{campaignName}"
	iphone1 = "iPhone 4/4s"
	iphone2 = "iPhone 5/5s"
	iphone3 = "iPhone 5c"




	if row[2] == "mail" then
		total = db.execute <<-SQL
		select count(*)
		from mailed
		SQL

		open("data/#{reportName}.txt", "w") do |file|
			file.write <<-TEXT
Report: 		     #{reportName}

	Date 			Total Mailed
	-------- 			-------------
	#{date}		#{total[0][0]}

TEXT
		end
	else
		counts = db.execute <<-SQL
		select count(*), deviceModel, campaignID
		from (
			select *
			from (
				select adjusted.*
				from adjusted
				where adjusted.uuid in (
					select uuid
					from mailed)
				)
			where campaignID = "#{campaignName}"
			)
		group by deviceModel
		SQL

		countsBoth = db.execute <<-SQL
		select count(*), deviceModel, campaignID
		from (
			select *
			from (
				select adjusted.*
				from adjusted
				where adjusted.uuid in (
					select uuid
					from mailed)
				)
			where campaignID = "Both"
			)
		group by deviceModel
		SQL

		total1 = counts[0][0] + countsBoth[0][0]
		total2 = counts[1][0] + countsBoth[1][0]
		total3 = counts[2][0] + countsBoth[2][0]
		sumTotal = total1 + total2 + total3


		open("data/#{reportName}.txt", "w") do |file|
			file.write <<-TEXT
Report: 		     #{reportName}

	Model 			Count
	-------- 			-------
	#{iphone1}		#{total1}
	#{iphone2}		#{total2}
	#{iphone3}		#{total3}
	-------- 			-------
	Total			#{sumTotal}
TEXT
		end
	end
end

db.execute <<-SQL
	update reportinfo
	set reported = "true"
	where reported = "false"
SQL
