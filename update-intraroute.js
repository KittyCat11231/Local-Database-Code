require('dotenv').config();

const { MongoClient } = require('mongodb');
const url = require('./atlas_uri');

const { google } = require('googleapis');

const googleSheets = google.sheets({
    version: 'v4',
    auth: process.env.GOOGLE_SHEETS_API_KEY
})

let spreadsheetIds = {
    pathfinding: '1w_Un6x3mglh2jpRTl4LrlBUMNYs6TbaFZMeN7NFllsY',
    ui_stops: '19Rz6TCs5rGDBuS4mZfN5R6axIINSoZxgjQgpaEVVxgg',
    ui_routes: '1cKR9eiEqjnvGEsxX3JYUiMCJRQIHKhcLL1_-q3_V7A8'
}

const client = new MongoClient(uri);

const dbname = 'felineHoldings';

async function connectToDatabase() {
    try {
        await client.connect();
        console.log(`Connected to the ${dbname} database.`);
    } catch (error) {
        console.error(`Error connecting to the ${dbname} database.`);
        console.error(error);
    }
}

async function getDataFromOneSheet(sheetType, mode) {
    try {
        const sheetTypeMap = new Map();
        sheetTypeMap.set('pathfinding', spreadsheetIds.pathfinding);
        sheetTypeMap.set('ui stops', spreadsheetIds.ui_stops);
        sheetTypeMap.set('ui routes', spreadsheetIds.ui_routes);
    
        let id = sheetTypeMap.get(sheetType);

        let response = await googleSheets.spreadsheets.values.get({
            'spreadsheetId': id,
            'range': mode
        });

        return response.data.values;

    } catch (error) {
        console.error(error);
        return null;
    }
}

async function updateDataForOneMode(mode) {
    try {
        let pathfindingData = await getDataFromOneSheet('pathfinding', mode);
        let uiStopsData = await getDataFromOneSheet('ui stops', mode);
        let uiRoutesData = await getDataFromOneSheet('ui routes', mode);

        class Stop {
            constructor(id, mode) {
                this.id = id;
                this.mode = mode;
            }
            adjacentStops = [];
            city;
            stopName;
            code;
            keywords;
            routes = [];
        }

        class AdjStop {
            constructor(id, weight, routes) {
                this.id = id;
                this.weight = weight;
                this.routes = routes;
            }
        }

        class RouteInStop {
            constructor(id, meta1, meta2) {
                this.id = id;
                this.meta1 = meta1;
                this.meta2 = meta2;
            }
        }

        let allStops = [];
        let stopsMap = new Map();

        pathfindingData.forEach(row => {
            if (row.length === 0) {
                return;
            }
            let thisStopId = row[2];
            let adjStopId = row[3];
            let adjStopWeight = row[4];
            let adjStopRoutes = [];
            for (let i = 5; i < row.length; i++) {
                let routeId = row[i];
                if (routeId.includes('to')) {
                    routeId = routeId.split('to')[0];
                }
                adjStopRoutes.push(routeId);
            }

            let thisAdjStop = new AdjStop(adjStopId, adjStopWeight, adjStopRoutes);
            let existingStop = stopsMap.get(thisStopId);

            if (!existingStop) {
                newStop = new Stop(thisStopId, mode);
                newStop.adjacentStops.push(thisAdjStop);
                allStops.push(newStop);
                stopsMap.set(thisStopId, newStop)
            } else {
                existingStop.adjacentStops.push(thisAdjStop);  
            }
        })

        uiStopsData.forEach(row => {
            if (row.length === 0) {
                return;
            }
            let thisStopId = row[2];
            let city = row[0];
            let stopName = row[1];
            let code = row[3];
            let routeId = row[4];
            if (routeId.includes('to')) {
                routeId = routeId.split('to')[0];
            }
            let meta1 = row[5];
            let meta2 = row[6];
            let keywords = [];
            for (let i = 7; i < row.length; i++) {
                keywords.push(row[i]);
            }

            let thisRoute = new RouteInStop(routeId, meta1, meta2);

            let existingStop = stopsMap.get(thisStopId);

            if (!existingStop) {
                console.error('Existing stop expected but not found:', thisStopId);
            } else {
                if (!existingStop.city) {
                    existingStop.city = city;
                    existingStop.stopName = stopName;
                    existingStop.code = code;
                    existingStop.keywords = keywords;
                }
                existingStop.routes.push(thisRoute);
            }
        })

        class Route {
            constructor(id, mode, type, bullet, num, altText, routeName, destinationId, destinationCity, destinationStopName, useFullNameIn, codeshares) {
                this.id = id;
                this.mode = mode;
                this.type = type;
                this.bullet = bullet;
                this.num = num;
                this.altText = altText;
                this.routeName = routeName;
                this.destinationId = destinationId;
                this.destinationCity = destinationCity;
                this.destinationStopName = destinationStopName;
                this.useFullNameIn = useFullNameIn;
                this.codeshares = codeshares;
            }
        }

        let allRoutes = [];

        uiRoutesData.forEach(row => {
            let id = null;
            let type = null;
            let bullet = null;
            let num = null;
            let altText = null;
            let routeName = null;
            let destinationId = null;
            let destinationCity = null;
            let destinationStopName = null;
            let useFullNameIn = [];
            let codeshares = [];

            if (mode === 'bahn' || mode === 'rail') {
                id = row[0];
                type = row[1];
                bullet = row[2];
                altText = row[3]
                routeName = row[4];
                destinationId = row[5];
                destinationCity = row[6];
                destinationStopName = row[7];
                for (let i = 8; i < row.length; i++) {
                    useFullNameIn.push(row[i]);
                }
            } else if (mode === 'air') {
                id = row[2];
                if (id.includes('to')) {
                    id = id.split('to')[0];
                }
                type = row[3];
                for (let i = 5; i < row.length; i += 2) {
                    codeshares.push(row[i]);
                }
            } else if (mode === 'sail') {
                id = row[0];
                type = row[1];
                num = row[2];
                routeName = row[3];
                destinationId = row[4];
                destinationCity = row[5];
                destinationStopName = row[6];
                for (let i = 7; i < row.length; i++) {
                    useFullNameIn.push(row[i]);
                }
            } else if (mode === 'bus' || mode === 'omega') {
                id = row[0];
                num = row[1];
                destinationId = row[2];
                destinationCity = row[3];
                destinationStopName = row[4];
                for (let i = 5; i < row.length; i++) {
                    useFullNameIn.push(row[i]);
                }
            } else if (mode === 'railScar' || mode === 'railLumeva') {
                id = row[0];
                num = row[1];
                routeName = row[2];
                destinationId = row[3];
                destinationCity = row[4];
                destinationStopName = row[5];
                for (let i = 6; i < row.length; i++) {
                    useFullNameIn.push(row[i]);
                }
            }

            allRoutes.push(new Route(id, mode, type, bullet, num, altText, routeName, destinationId, destinationCity, destinationStopName, useFullNameIn, codeshares))
        })

        let dbStopsCollectionName;
        let dbRoutesCollectionName;

        if (mode === 'bahn') {
            dbStopsCollectionName = 'intraBahnStops';
            dbRoutesCollectionName = 'intraBahnRoutes';
        } else if (mode === 'air') {
            dbStopsCollectionName = 'intraAirStops';
            dbRoutesCollectionName = 'intraAirRoutes';
        } else if (mode === 'rail') {
            dbStopsCollectionName = 'intraRailStops';
            dbRoutesCollectionName = 'intraRailRoutes';
        } else if (mode === 'sail') {
            dbStopsCollectionName = 'intraSailStops';
            dbRoutesCollectionName = 'intraSailRoutes';
        } else if (mode === 'bus') {
            dbStopsCollectionName = 'intraBusStops';
            dbRoutesCollectionName = 'intraBusRoutes';
        } else if (mode === 'omega') {
            dbStopsCollectionName = 'omegaBusStops';
            dbRoutesCollectionName = 'omegaBusRoutes';
        } else if (mode === 'railScar') {
            dbStopsCollectionName = 'irtScarStops';
            dbRoutesCollectionName = 'irtScarRoutes';
        } else if (mode === 'railLumeva') {
            dbStopsCollectionName = 'irtLumevaStops';
            dbRoutesCollectionName = 'irtLumevaRoutes';
        }

        let dbStopsCollection = client.db(dbname).collection(dbStopsCollectionName);
        let dbRoutesCollection = client.db(dbname).collection(dbRoutesCollectionName);

        dbStopsCollection.drop();
        dbRoutesCollection.drop();

        dbStopsCollection.insertMany(allStops);
        dbRoutesCollection.insertMany(allRoutes);

        console.log(`Added ${mode} data to the database.`)

    } catch (error) {
        console.error(error);
    }
}

async function updateFromSheets() {
    try {
        updateDataForOneMode('bahn');
        updateDataForOneMode('air');
        updateDataForOneMode('rail');
        updateDataForOneMode('sail');
        updateDataForOneMode('bus');
        updateDataForOneMode('omega');
        updateDataForOneMode('railScar');
        updateDataForOneMode('railLumeva');
    } catch (error) {
        console.error(error);
    }
}

async function master() {
    connectToDatabase()
    .then(updateFromSheets())
    .then(client.close());
}

master();