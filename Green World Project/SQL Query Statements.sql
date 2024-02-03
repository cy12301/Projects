use greenworld2022;

## Our world in data energy data
##	How many countries are captured in [owid_energy_data]
select count(distinct iso_code) as Number_of_Countries from owid_energy_data
where iso_code != "" AND iso_code NOT LIKE 'OWID%';


## Earliest and latest year in [owid_energy_data]
select MAX(CAST(SUBSTRING(year,1,5) as UNSIGNED)) as Latest_Year, MIN(CAST(SUBSTRING(year,1,5) as UNSIGNED)) as Earliest_Year
from owid_energy_data;

## countries having a record in [owid_energy_data] every year throughout the entire period
select country, count(distinct year) as Number_of_Years
from owid_energy_data
group by country
having count(distinct year) = 122;

##	Year that <fossil_share_energy> stopped being the full source of energy for Singapore
select MIN(CAST(SUBSTRING(year,1,5) as SIGNED)) as Year 
from owid_energy_data
where country = 'Singapore'
and CAST(SUBSTRING(fossil_share_energy,1,4) as DECIMAL(6,3)) < 100;

##  new sources of energy
with fossilenergy as 
(select MIN(CAST(SUBSTRING(year,1,5) as SIGNED)) as Year 
from owid_energy_data
where country = 'Singapore'
and CAST(SUBSTRING(fossil_share_energy,1,4) as DECIMAL(6,3)) < 100
)
select fossil_share_energy, other_renewables_share_energy, hydro_share_energy, low_carbon_share_energy, nuclear_share_energy, solar_share_energy, wind_share_energy from owid_energy_data
where year in (select * from fossilenergy)
and country = "Singapore";
##from the results, low carbon energy and other renewables energy are of the same values and adds up to 100% with fossil energy, and since low-carbon energy sources are described to be from renewables and nuclear,
##other renewables and low carbon having the same value of 0.143% is because they refer to the same group of energy

## average GDP of ASEAN countries from 2000 to 2021 desc
SELECT country, year, AVG(CONVERT(GDP,Signed)) AS `GDP`
FROM owid_energy_data
WHERE country IN ("Brunei","Cambodia","Indonesia","Laos","Malaysia",
"Myanmar","Philippines","Singapore","Thailand","Vietnam")
AND Year BETWEEN "2000" AND "2021"
GROUP BY country, year
ORDER BY AVG(CONVERT(GDP,Signed)) DESC;

## Oil Consumption 3-Year Moving Average for each ASEAN country, instances of negative change
SELECT country, year, AVG(CONVERT(oil_consumption, Signed)) 
OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS `Oil Consumption Moving Average 3-Years`
FROM owid_energy_data
WHERE country IN ("Brunei","Cambodia","Indonesia","Laos","Malaysia",
"Myanmar","Philippines","Singapore","Thailand","Vietnam")
AND Year BETWEEN "2000" AND "2019";
/* 2000-2002, 2001-2003, 2002-2004, 2003-2005, 2004-2006, 2005-2007, 2006-2008, 2007-2009, 2008-2010, 2009-2011, 2010-2012, 2011-2013
2012-2014, 2013-2015, 2014-2016, 2015-2017, 2016-2018, 2017-2019, 2018-2020, 2019-2021 (20) */

## GDP 3-Year Moving Average
SELECT country, year, AVG(CONVERT(GDP, Signed)) OVER (PARTITION BY country ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS `GDP 3-Year Moving Average`
FROM owid_energy_data
WHERE country IN ("Brunei","Cambodia","Indonesia","Laos","Malaysia",
"Myanmar","Philippines","Singapore","Thailand","Vietnam")
AND Year BETWEEN "2000" AND "2019";

## overall_avg_ktoe_energy_products of importsofenergyproducts
select energy_products, avg(avgEnergyProducts) as overall_avg_ktoe_energy_products from (select year, energy_products, avg(value_ktoe) as avgEnergyProducts from importsofenergyproducts group by year, energy_products) ener
group by energy_products;

## overall_avg_ktoe_sub_products of importsofenergyproducts
select sub_products, avg(avgSubProducts) as overall_avg_ktoe_sub_products from (select year, sub_products, avg(value_ktoe) as avgSubProducts from importsofenergyproducts group by year, sub_products) sub
group by sub_products;

## overall_avg_ktoe_energy_products of exportsofenergyproducts
select energy_products, avg(avgEnergyProducts) as overall_avg_ktoe_energy_products from (select year, energy_products, avg(value_ktoe) as avgEnergyProducts from exportsofenergyproducts group by year, energy_products) ener
group by energy_products;

## overall_avg_ktoe_sub_products of exportsofenergyproducts
select sub_products, avg(avgSubProducts) as overall_avg_ktoe_sub_products from (select year, sub_products, avg(value_ktoe) as avgSubProducts from exportsofenergyproducts group by year, sub_products) sub
group by sub_products;

## YEARLY DIFFERENCE of ALL COMBINATIONS, BEFORE IDENTIFYING AND FILTERING FOR EXPORT VALUE > IMPORT VALUE
select ex.year, ex.energy_products, ex.sub_products, (ex.value_ktoe - im.value_ktoe) as diff_in_ktoe
from exportsofenergyproducts ex, importsofenergyproducts im
where ex.year = im.year and ex.energy_products = im.energy_products and ex.sub_products = im.sub_products; 


## FINAL VERSION AFTER IDENTIFYING YEAR(S) WHERE THERE ARE MORE THAN 4 INSTANCE OF EXPORT VALUE > IMPORT VALUE : 
select ex.year, ex.energy_products, ex.sub_products, ex.value_ktoe as export_value, im.value_ktoe as import_value, (ex.value_ktoe - im.value_ktoe) as diff_in_ktoe
from exportsofenergyproducts ex, importsofenergyproducts im
where ex.year = im.year 
and ex.energy_products = im.energy_products 
and ex.sub_products = im.sub_products 
and ex.value_ktoe - im.value_ktoe > 0	# export value > import value
group by ex.year, ex.energy_products, ex.sub_products, ex.value_ktoe, im.value_ktoe, diff_in_ktoe
having ex.year in 
(
	select year from	# years with more than 4 instances of export > import 
	(
		select ex.year, (ex.value_ktoe - im.value_ktoe) as diff_in_ktoe
		from exportsofenergyproducts ex, importsofenergyproducts im
		where ex.year = im.year 
        and ex.energy_products = im.energy_products 
        and ex.sub_products = im.sub_products 
        and ex.value_ktoe - im.value_ktoe > 0
	) tabl
	group by year
	having count(year) > 4
); 

# EMA Singapore Energy Consumption data

## yearly average <kwh_per_acc> in [householdelectricityconsumption]
describe householdelectricityconsumption;

select * from householdelectricityconsumption where month = "Annual" and kwh_per_acc = 0;
#turn off safe mode if it is in safe mode
SET SQL_SAFE_UPDATES = 0;
## delete s from the kwh_per_acc column
delete from householdelectricityconsumption where month = "Annual" and kwh_per_acc = 0;
## optional to turn back on safe mode
## SET SQL_SAFE_UPDATES = 1;
## take monthly average * 12 to get yearly average
## Use overall dwelling_type, since there may be different number of dwelling type per description/subregion affecting the overall number
select Region, year, avg(kwh_per_acc*12) Average_yearly_kwh_per_acc from householdelectricityconsumption WHERE region != "Overall" and dwelling_type="Overall" and month="Annual" group by Region, year order by Region, year;

## Top 3 regions with the most instances of negative 2-year averages

select region, count(r.Average_yearly_kwh2-f.Average_yearly_kwh1) no_of_moving_2_year_average_difference from (select year-2004 year, Region region, avg(kwh_per_acc*12) Average_yearly_kwh1 from householdelectricityconsumption WHERE region != "Overall" and dwelling_type="Overall" and month="Annual" and year!=2021 group by Region, year) f natural join
(select year-2005 year, Region region, avg(kwh_per_acc*12) Average_yearly_kwh2 from householdelectricityconsumption WHERE region != "Overall" and dwelling_type="Overall" and month="Annual" and year!=2005 group by Region, year) r where r.year=f.year and r.Average_yearly_kwh2-f.Average_yearly_kwh1<0 group by region order by count(r.Average_yearly_kwh2-f.Average_yearly_kwh1) desc LIMIT 3;

## Quarterly average in <kwh_per_acc>

SELECT Region, CAST(year AS DECIMAL(10,0)) AS year, FLOOR(CAST((month-1)/3+1 AS DECIMAL(10,2))) AS quarter, AVG(CAST(kwh_per_acc AS DECIMAL(10,2))) AS avg_kwh_per_acc FROM 
(SELECT * FROM `householdelectricityconsumption`
WHERE kwh_per_acc !=0) as householdeconsumption
GROUP BY Region, year, quarter
HAVING quarter in (1,2,3,4) AND Region NOT IN ("Overall") 
ORDER BY Region, year, quarter;

## quarterly average in <avg_mthly_hh_tg_consp_kwh> for each <sub_housing_type>

SELECT sub_housing_type, CAST(year AS DECIMAL(10,0)) AS year, FLOOR(CAST((month-1)/3+1 AS DECIMAL(10,2))) AS quarter, AVG(CAST(avg_mthly_hh_tg_consp_kwh AS DECIMAL(10,2))) AS avg_quarterly_hh_tg_consp_kwh FROM 
(SELECT * FROM `householdtowngasconsumption`
WHERE avg_mthly_hh_tg_consp_kwh!=0) as householdgconsumption
GROUP BY sub_housing_type, year, quarter
HAVING quarter in (1,2,3,4) AND sub_housing_type NOT IN ("Overall")
ORDER BY sub_housing_type, year, quarter;