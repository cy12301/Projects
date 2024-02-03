//Our world in data energy data//
//How many countries are captured in [owid_energy_data]//
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $match: {$and: [{"data.isocode" : {$not : /OWID/}}, {"data.isocode" : {$ne: ""}}]}},
    { $group: {_id:"$_id"}},
    { $count: "Number of Countries"}
    ])
    
//earliest and latest year in OWID database//
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $group : {_id: "$year",earliestyear: {$min:"$data.year"}, latestyear: {$max: "$data.year"}}},
    { $project: {_id:0, earliestyear:1, latestyear:1}}
    ])
    
//number of records between 1900 and 2021//
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $match: {$or: [{"data.year": {$gt: 1900}}, {"data.year": {$lt: 2021}}]}},
    { $project: {_id:0, year: "$data.year"}},
    { $group: {_id:"$year"}},
    { $count: "number of years"},
    ])

//number of years = 122, finding countries with all years data record//
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $group: {_id:"$_id", numberofyears: {$count: {}}}},
    { $match: {
        numberofyears: 122
    }}
    ])
    
//Year that <fossil_share_energy> stopped being the full source of energy for Singapore//
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $match: {$and: [{_id: "Singapore"}, {"data.FossilSE": {$lt: 100}}]}},
    { $project: {_id:0, year: "$data.year"}},
    { $sort: {year: 1}},
    { $limit: 1}
    ])

//new sources of energy besides fossil fuel in 1986//  
db.owid_energy_data.aggregate([
    { $unwind: "$data"},
    { $match: {$and: [{_id: "Singapore"}, {"data.FossilSE": {$lt: 100}}]}},
    { $sort: {year: 1}},
    { $project: {
        _id:0, 
        year: "$data.year", 
        Fossil_Fuel_Energy: "$data.FossilSE",
        Low_Carbon_Energy: "$data.LowCarbonSE",
        Renewable_Energy: "$data.RenewablesSE",
        Other_Renewables_Energy: "$data.OtherRenewablesSE",
        Solar_Energy: "$data.SolarSE",
        Wind_Energy: "$data.WindSE",
        Nuclear_Energy: "$data.NuclearSE",
        Hydro_Energy: "$data.hydroSE",
    }},
    { $limit: 1}
    ])
//Low carbon energy, renewable energy and other renewables energy all refer to energy that is renewable or nuclear, hence referring to the same source of energy that has low carbon emissions//


//average GDP of ASEAN countries from 2000 to 2021 desc//
db.owid_energy_data.aggregate([
    {$unwind:"$data"},
    {$match:{_id: {$in:[
        "Brunei","Cambodia","Indonesia","Laos","Malaysia","Myanmar",
        "Philippines","Singapore","Thailand","Vietnam"]}}},
    {$match: {$and: [{"data.year": {$gt: 2000}}, {"data.year": {$lt: 2021}}]}},
    {$group:{_id: {country:'$_id', year:'$data.year',average_gdp: {$avg:"$data.gdp"}}}},
    {$sort: {average_gdp: -1}}
   ])

//Oil Consumption 3-Year Moving Average for each ASEAN country, instances of negative change//
db.owid_energy_data.aggregate([
    {$unwind:"$data"},
    {$addFields:{"data.country":"$_id"}},
    {$replaceRoot:{"newRoot": "$data"}},
    {$match:{country: {$in:["Brunei","Cambodia","Indonesia","Laos","Malaysia", "Myanmar","Philippines","Singapore","Thailand","Vietnam"]}}},
    {$match:{year:{$in:[
        2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,
        2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021]}}},
    {$group: {'_id': {country:'$country',year:'$year'}, data:{$push:{oil_consumption:"$oil_consumption"}}}},
    {$unwind:"$data"},
    {$setWindowFields: {
        sortBy: {_id:1},
        output: {
          tempid: {
            $count: {},
            window: { documents: ['unbounded', 'current'] },
          },
        },
      },
    },
    {$setWindowFields: {
        sortBy:{tempid:1},
        output: {
            average: {
                $push: "$data.oil_consumption", window: { range: [-2, "current"]}}}}},
    {$set: {average: { $avg:'$average'}}},
    {$match:{'_id.year':{$nin:[2000,2001]}}}
    ])  

//GDP 3-Year Moving Average//
db.owid_energy_data.aggregate([
    {$unwind:"$data"},
    {$addFields:{"data.country":"$_id"}},
    {$replaceRoot:{"newRoot": "$data"}},
    {$match:{country: {$in:["Brunei","Cambodia","Indonesia","Laos","Malaysia","Myanmar","Philippines","Singapore","Thailand","Vietnam"]}}},
    {$match:{year:{$in:[
        2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,
        2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021]}}},
    {$group: {'_id': {country:'$country',year:'$year'}, data:{$push:{gdp:"$gdp"}}}},
    {$unwind:"$data"},
    {$setWindowFields: {
        sortBy: {_id:1},
        output: {
          tempid: {
            $count: {},
            window: { documents: ['unbounded', 'current'] },
          },
        },
      },
    },
    {$setWindowFields: {
        sortBy:{tempid:1},
        output: {
            average: {
                $push: "$data.gdp", window: { range: [-2, "current"]}
      }
    }
  }},
  {$set: {average: { $avg:'$average'}}},
  {$match:{'_id.year':{$nin:[2000,2001]}}}
  ])

// Overall average of <value_ktoe> for <energy_products> from imports
db.imports_exports_linked.aggregate([
    {$group: {
        '_id': '$energy_products','average_energy_products_import_ktoe': {
            '$avg': "$imports_value_ktoe"}}},
    {$sort: {'_id': 1}}
   ])

// Overall average of <value_ktoe> for <sub_products> from imports
db.imports_exports_linked.aggregate([
    {$group: {
        '_id': '$sub_products','average_sub_products_import_ktoe': {
            '$avg': "$imports_value_ktoe"}}},
    {$sort: {'_id': 1}}
   ])

// Overall average of <value_ktoe> for <energy_products> from exports
db.imports_exports_linked.aggregate([
    {$match:{exports_value_ktoe:{$ne:null}}},
    {$group: {
        '_id': '$energy_products','average_energy_products_export_ktoe': {
            '$avg': "$exports_value_ktoe"}}},
    {$sort: {'_id': 1}}
   ])

// Overall average of <value_ktoe> for <sub_products> from exports
db.imports_exports_linked.aggregate([
    {$match:{exports_value_ktoe:{$ne:null}}},
    {$group: {
        '_id': '$sub_products','average_sub_products_export_ktoe': {
            '$avg': "$exports_value_ktoe"}}},
    {$sort: {'_id': 1}}
   ])

// Compare exports and imports and calculate yearly difference
db.imports_exports_linked.aggregate([
    {$group: {
            _id: {
            year: '$year', 
            energy_products: '$energy_products', 
            sub_products: '$sub_products', 
            export_is_greater: '$export_is_greater', 
            exports_value_ktoe: '$exports_value_ktoe', 
            imports_value_ktoe: '$imports_value_ktoe'}}},
    
// if difference negative, it means exports > imports
    {$project: {
        difference: {$subtract: [ "$_id.imports_value_ktoe" , "$_id.exports_value_ktoe" ]}}},  
    {$set: {'export_is_greater': {
        $sum:{ '$cond':{ 
            if:{
                $gt: [ "$_id.exports_value_ktoe", "$_id.imports_value_ktoe" ]},
            then: {$sum: 1}, else: {$sum: 0}}}}}},
            
    {$sort: {'_id': 1}},

// find documents where export is greater than import
    {$match: {export_is_greater: 1}},

// identify year(s) where there are more than 4 instances of exports > imports
    {$group: {
            _id: '$_id.year', count: {$sum: 1}}},
    {$match:{count:{$gt:4}}}
    ]) 



// EMA Singapore Energy Consumption data
// yearly average <kwh_per_acc> in [householdelectricityconsumption]
db.householdelectricityconsumption.aggregate([
    //unwinding and unembedding documents to get back original documents without "s" in kwh_per_acc
    {$unwind:"$data"},{$unwind:"$data.data"},
    {$replaceRoot:{newRoot:{region:"$region",description:"$description",year:"$data.year",dwelling_type:"$data.data.dwelling_type",month:"$data.data.month", kwh_per_acc:"$data.data.kwh_per_acc"}}},
    //remove overall region, and use annual monthly and overall dwelling_type
    {$match:{$and:[{region:{$nin:["Overall"]}},{dwelling_type:{$eq:"Overall"}},{month:"Annual"}]}},
    //multiply monthly average by 12 to get yearly average
    {$project:{region:1,year:1,dwelling_type:1,description:1,month:1,kwh_per_acc:{$multiply:["$kwh_per_acc",12]}}},
    //find avg in each region and year
    {$group:{_id:{region:"$region",year:"$year"},avgkwh_per_acc:{$avg:"$kwh_per_acc"}}},
    //order by _id.region then _id.year, or $sort:{_id:1} also get same result
    {$sort:{_id:1}}//{$sort:{"_id.region":1,"_id.year":1}}
    ]);

// Top 3 regions with the most instances of negative 2-year averages
db.householdelectricityconsumption.aggregate([
    // unwinding and unembedding documents to get back original documents without "s" in kwh_per_acc
    {$unwind: "$data"},
    {$unwind: "$data.data"},
    {$replaceRoot: {newRoot: {
        region: "$region", 
        description: "$description", 
        year: "$data.year", 
        dwelling_type: "$data.data.dwelling_type", 
        month: "$data.data.month", 
        kwh_per_acc: "$data.data.kwh_per_acc"}}},
    // remove region=overall, and use annual monthly and overall dwelling_type
    {$match: {$and: [{
        region: {
            $nin: ["Overall"]}},
        {dwelling_type: {$eq: "Overall"}},
        {month: "Annual"}]}},
    // multiply monthly average by 12 to get yearly average
    {$project: {region:1, year:1, kwh_per_acc: {$multiply: ["$kwh_per_acc",12]}}},
    // find avg in each region and year
    {$group: {_id: {
        region: "$region",
        year: "$year"},
        avgkwh_per_acc: {
            $avg:"$kwh_per_acc"}}},
    // create a running number (tempid) to do range in next $setWindowFields
    {$setWindowFields: {
        sortBy: {_id:1},
        output: {
          tempid: {
            $count: {},
            window: { documents: ['unbounded', 'current']},
          },
        },
      },
    },
    // add previous year's avgkwh_per_acc into difference using tempid
    {$setWindowFields: {
        sortBy: {tempid:1},
        output: {
            difference: {
                $push: "$avgkwh_per_acc",
                window: {range: [-1, "current"]}}}}},
  // remove the year=2005 ones since those difference uses the previous region's kwh_per_acc value in 2021, while the first one will only have one value in difference
  {$match: {"_id.year":{$ne:2005}}},
  // find difference by subtracting the 2 values
  {$set: {
    difference: { $subtract: [{ $last: "$difference" }, { $first: "$difference" }] }
  }},
  // get all the difference that is negative
    {$match: {difference:{$lt:0}}},
    // group according to region and count number of negative difference
    {$group: {_id:"$_id.region", count:{$sum:1}}},
    // sort desc and show top 3
    {$sort: {count:-1}},{$limit:3}
]);

// Quarterly average in <kwh_per_acc>
db.householdelectricityconsumption.aggregate([
    {$match: {"region": {$nin: ["Overall"]}}},
    {$unwind: "$data"}, 
    {$unwind: "$data.data"},
    {$replaceRoot: {newRoot: {
        region: "$region",
        description: "$description",
        year: "$data.year",
        dwelling_type: "$data.data.dwelling_type",
        month: "$data.data.month", 
        kwh_per_acc: "$data.data.kwh_per_acc"}}},
    {$match: {"month": {$nin: ["Annual"]}}},
    {$project: {
        region:1,
        description:1,
        year:1,
        dwelling_type:1,
        kwh_per_acc:1,
        month:{$toInt:"$month"}}},
    {$project: {
      region:1,
      description:1,
      year:1,
      dwelling_type:1,
      kwh_per_acc:1,
      quarter: {
        $cond: [
          { $lte: ["$month", 3] },
          1,
          {
            $cond: [
              { $lte: ["$month", 6] },
              2,
              {
                $cond: [{ $lte: ["$month", 9] }, 3, 4],
              },
            ],
          },
        ],
      },
    },
    },
    {$group: { _id: { region:"$region", year:"$year",quarter: "$quarter" }, avg: { $avg : "$kwh_per_acc" } } },
    {$sort: {"_id.year":1, "_id.quarter":1,"_id.sub_housing_type":1}},
    ])

// quarterly average in <avg_mthly_hh_tg_consp_kwh> for each <sub_housing_type>    
db.householdtowngasconsumption.aggregate([
    {$match: {"sub_housing_type": {$nin: ["Overall"]}}},
    {$unwind: "$data"}, 
    {$unwind: "$data.data"},
    {$replaceRoot: {newRoot: {
        housing_type:"$housing_type",
        sub_housing_type:"$sub_housing_type",
        year:"$data.year",
        avg_mthly_hh_tg_consp_kwh:"$data.data.avg_mthly_hh_tg_consp_kwh",
        month:"$data.data.month"}}},
    {$project: {
      month: 1,
      housing_type:1,
      sub_housing_type:1,
      year:1,
      avg_mthly_hh_tg_consp_kwh:1,
      quarter: {
        $cond: [
          { $lte: ["$month", 3] },
          1,
          {
            $cond: [
              { $lte: ["$month", 6] },
              2,
              {
                $cond: [{ $lte: ["$month", 9] }, 3, 4],
              },
            ],
          },
        ],
      },
    },
    },
    {$group: {_id: {
        sub_housing_type:"$sub_housing_type", 
        year:"$year",quarter: "$quarter" }, 
        Average: { $sum: "$avg_mthly_hh_tg_consp_kwh" } } },
    {$sort:{"_id.year":1, "_id.quarter":1,"_id.sub_housing_type":1}},
    {$replaceRoot:{newRoot: {
        sub_housing_type:"$_id.sub_housing_type",
        year:"$_id.year",
        avg_mthly_hh_tg_consp_kwh:"$Average",
        quarter:"$_id.quarter"}}},
    {$group: {_id: {
        sub_housing_type:"$sub_housing_type", 
        year:"$year"}, 
        Average: {$avg: "$avg_mthly_hh_tg_consp_kwh"} } },
    {$sort:{"_id.year":1, "_id.quarter":1,"_id.sub_housing_type":1}}
    ])