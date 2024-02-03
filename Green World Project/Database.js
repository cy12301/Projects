// database implementation//

// householdelectricityconsumption collection
// remove \r from kwh_per_acc//
db.householdelectricityconsumption.updateMany({}, 
[{$set:{kwh_per_acc:{$replaceOne:{input:"$kwh_per_acc",find:"\r",replacement:""}}}}
]);

db.householdelectricityconsumption.aggregate([
    // remove documents with value "s"
    {$match:{kwh_per_acc:{$nin:["s"]}}},
    // convert kwh_per_acc to decimal and year to Int
    {$project:{dwelling_type:1,month:1,Region:1,Description:1,year:{$toInt:"$year"},kwh_per_acc:{$toDecimal:"$kwh_per_acc"}}},
    // push dwelling_type, month and kwh_per_acc into an array
    {$group: { _id:{"region":"$Region", "description":"$Description", "year":"$year"}, data:{$push:{dwelling_type:"$dwelling_type", month:"$month", kwh_per_acc:"$kwh_per_acc"}} } },
    // sort according to the year
    {$sort:{_id:1}},
    // push data further into arrays by year
    {$group: { _id:{region:"$_id.region", description:"$_id.description"}, data:{$push:{year:"$_id.year", data:"$data" }} } },
    // expand _id
    {$replaceRoot: {newRoot:{region:"$_id.region", description:"$_id.description", data:"$data"}}},
    {$out: "householdelectricityconsumption"}
    ])

// HouseholdTownGasConsumption Collection
// Remove data that does not coincide with the field.
db.householdtowngasconsumption.remove({housing_type:{$nin:["Public Housing","Private Housing", "Overall"]}})

db.householdtowngasconsumption.remove({month:"na"})

// Remove \r from the end of the string in avg_mthly_hh_tg_consp_kwh
db.householdtowngasconsumption.updateMany({}, 
[{$set:{avg_mthly_hh_tg_consp_kwh:{$replaceOne:{input:"$avg_mthly_hh_tg_consp_kwh",find:"\r",replacement:""}}}}
]);

// Implementation
db.householdtowngasconsumption.aggregate([
    // remove overall and documents with value "s", and use annual monthly average
    {$match:{avg_mthly_hh_tg_consp_kwh:{$nin:["s"]}}},
    // convert avg_mthly_hh_tg_consp_kwh to decimal
    {$project:{housing_type:1,sub_housing_type:1,
        year:{$toInt:"$year"},
        month:{$toInt: "$month"},
        avg_mthly_hh_tg_consp_kwh:{$toDecimal:"$avg_mthly_hh_tg_consp_kwh"}}},
    {$out: "householdtowngasconsumption"}
])

db.householdtowngasconsumption.aggregate([
    {$group: {_id: {
        "housing_type":"$housing_type", 
        "sub_housing_type":"$sub_housing_type", 
        "year":"$year"}, 
    data: {$push: {
        month:"$month", 
        avg_mthly_hh_tg_consp_kwh:"$avg_mthly_hh_tg_consp_kwh"}}}},
    {$sort:{_id:1}},
    {$group: { _id: {
        housing_type:"$_id.housing_type", 
        sub_housing_type:"$_id.sub_housing_type"}, 
        datas:{$push:{year:"$_id.year", data:"$data" }} } },
    {$replaceRoot: {newRoot: {
        housing_type:"$_id.housing_type", 
        sub_housing_type:"$_id.sub_housing_type", 
        data:"$datas"}}},
    {$out: "householdtowngasconsumption"}
])


// Combining Exports and Imports of energy products for Q6 & 7
db.exportsofenergyproducts.aggregate([
    {$out:"imports_exports_linked"}])
db.imports_exports_linked.updateMany({},{$rename:{"value_ktoe":"exports_value_ktoe"}})
db.importsofenergyproducts.aggregate([
    {$merge:"imports_exports_linked"}])
db.imports_exports_linked.updateMany({},{$rename:{"value_ktoe":"imports_value_ktoe"}})
db.imports_exports_linked.aggregate([
    {$group:{_id:{year:"$year",energy_products:"$energy_products", sub_products:"$sub_products"}, data:{$push: "$$ROOT"}}},
    {$project:{_id:1, "data.imports_value_ktoe":1,"data.exports_value_ktoe":1}},
    {$replaceRoot:{newRoot:{year:"$_id.year",energy_products:"$_id.energy_products", sub_products:"$_id.sub_products", exports_value_ktoe:{$first:"$data.exports_value_ktoe"}, imports_value_ktoe:{$first:"$data.imports_value_ktoe"}}}},
    {$project:{year:1,energy_products:1,sub_products:1,exports_value_ktoe:{$toDouble:"$exports_value_ktoe"},imports_value_ktoe:{$toDouble:"$imports_value_ktoe"}}
    {$out:"imports_exports_linked"}
    ]);
    
// Drop original collections of importsofenergyproducts and exportsofenergyproducts to remove redundant collections 
db.importsofenergyproducts.drop()
db.exportsofenergyproducts.drop()


// OWID Data Collection
db.owid_energy_data.aggregate([
    { $group: {
        _id: "$country",
        data: {$push: {
            isocode: "$iso_code",
            year: {$toInt: "$year"}, 
            gdp: {$convert: {input: "$gdp", to: "decimal", onError: 0, onNull: 0}}, 
            population: {$convert: {input: "$population", to: "decimal", onError: 0, onNull: 0}},
            oilconsumption: {$convert: {input: "$oil_consumption", to: "decimal", onError: 0, onNull: 0}},
            FossilSE: {$convert: {input: "$fossil_share_energy", to: "decimal", onError: 0, onNull: 0}},
            OilSE: {$convert: {input: "$oil_share_energy", to: "decimal", onError: 0, onNull: 0}},
            GasSE: {$convert: {input: "$gas_share_energy", to: "decimal", onError: 0, onNull: 0}},
            LowCarbonSE: {$convert: {input: "$low_carbon_share_energy", to: "decimal", onError: 0, onNull: 0}},
            RenewablesSE: {$convert: {input: "$renewables_share_energy", to: "decimal", onError: 0, onNull: 0}},
            OtherRenewablesSE: {$convert: {input: "$other_renewables_share_energy", to: "decimal", onError: 0, onNull: 0}},
            SolarSE: {$convert: {input: "$solar_share_energy", to: "decimal", onError: 0, onNull: 0}},
            WindSE: {$convert: {input: "$wind_share_energy", to: "decimal", onError: 0, onNull: 0}},
            NuclearSE: {$convert: {input: "$nuclear_share_energy", to: "decimal", onError: 0, onNull: 0}},
            HydroSE: {$convert: {input: "$hydro_share_energy", to: "decimal", onError: 0, onNull: 0}},
        }}
        }},
    { $sort: {_id: 1}},
    { $out: "owid_energy_data"}
    ])