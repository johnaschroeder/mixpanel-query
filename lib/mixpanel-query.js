#! /usr/bin/env node


/*
 * mixpanel-query
 * https://github.com/johnaschroeder/mixpanel-query
 *
 * Copyright (c) 2014 John Schroeder
 * Licensed under the MIT license.
 */

'use strict';

var request = require("request");
var md5 = require('MD5');
var MongoClient = require("mongodb").MongoClient;
var __ = require('underscore');
var userArgs = process.argv.slice(2);

var peopleUrl = "http://mixpanel.com/api/2.0/engage/";
var eventUrl = "https://data.mixpanel.com/api/2.0/export/";
var apiSecret = "83d6ef6b32aa6f1146b0a88563ea8602";

var peopleQueryParams = {"api_key" : "c4ed82d9d6e9ec2ab8b9660740dd09fb",
                         "expire" :  "1806564626"};

var eventQueryParams = {"api_key" : "c4ed82d9d6e9ec2ab8b9660740dd09fb",
                        "expire" :  "1806564626",
                        "from_date": "2014-04-01",
                        "to_date": "2014-07-29",
                        "event": '["Added Content","New Sphere","Loaded App","Added Collaborator","Sphere Shared","1st Login","Intro Video","Getting Started Video"]'};

function hashQueryString(qsString, secret) {
    console.log('qsString: ', qsString);
    var mpSig = md5(qsString + secret);
    return mpSig;
}


//sort by key for JSON and js objects
function sortByKey (myObj) {
    var qsKeys = Object.keys(myObj);
    qsKeys.sort();

    var i, len = qsKeys.length, k, kvPair;
    var sortedKeyString = "";
    for (i = 0; i < len; i++) {
        k = qsKeys[i];
        kvPair = k + "=" + myObj[k];
        sortedKeyString += kvPair;
    }
    return sortedKeyString;
}

var options = {
    method: 'GET'
};


//    qs: {'api_key': apiKey, 'expire': expireTime, 'sig': mpSig}


function returnValidFields(people) {
    var arrayLength = people.length;
    var thisId, thisProperties, propertyLength;
    for (var i = 0; i < arrayLength; i++) {
        thisId = people[i]["$distinct_id"];
        people[i]["_id"] = thisId;
        delete people[i]["$distinct_id"];
        thisProperties = people[i]["$properties"];
        people[i]["properties"] = thisProperties;
        delete people[i]["$properties"];
        if(people[i].properties && people[i].properties.id) {
            people[i].zimid = people[i].properties.id;
        }
        if (people[i].properties && people[i].properties.emails) {
            people[i].email = people[i].properties.emails[0]
        }
    }
    return people;
}

function returnValidPropertyFields(events) {
    var propertiesToClean = {'$browser': 'browser',
                             '$city': 'city',
                             '$initial_referrer': 'initial_referrer',
                             '$initial_referring_domain': 'initial_referring_domain',
                             '$os': 'os',
                             '$referrer': 'referrer',
                             '$referring_domain': 'referring_domain',
                             '$device': 'device',
                             '$region': 'region',
                             '$screen_height': "screen_height",
                             '$screen_width': "screen_width",
                             '$search_engine': 'search_engine',
                            };
    var thisProperty;
    __.map(events, function(thisEvent){
        __.map(propertiesToClean, function(val, key){
            thisProperty = thisEvent.properties[key];
            if(thisProperty) {
                thisEvent.properties[val] = thisProperty;
                delete thisEvent.properties[key];
            }
        });
    });
    return events;
}

function removeDuplicateRecords(people) {
    var arrResult = {};
    var nonDuplicatedArray = [];
    var person;
    var n = people.length;
    for (var i = 0; i < n; i++) {
        person = people[i];
        arrResult[person["$distinct_id"]] = person;
    }
    // console.log('arrResult: ', arrResult);
    //Then you just loop the arrResult again, and recreate the array.
    i = 0;
    for(var item in arrResult) {
        nonDuplicatedArray[i] = arrResult[item];
        i++;
    }
    return nonDuplicatedArray;
}


function saveEvents(events) {
    MongoClient.connect('mongodb://127.0.0.1:27017/mixpanel', function(err, db) {
        if (err) {
            throw err;
        }
        console.log("Connected to Database");

        //update record with events
        var myCollection = db.collection('people');
        var userid, eventName;
        __.map(events, function(thisEvent){
            userid = thisEvent.properties.distinct_id;
            eventName = thisEvent.properties.time;
            // console.log('userid: ', userid, ' eventName: ', eventName);
            // console.log('thisEvent: ', thisEvent);
            var modifier = { $push: {} };
            modifier.$push["events"] = thisEvent;
            myCollection.update({_id: userid}, modifier, function(err, records) {
                if (err) {
                    throw err;
                } else {
                    // console.log('records: ', records);
                }
            });
        });
    });
}

function savePeople(people) {
    MongoClient.connect('mongodb://127.0.0.1:27017/mixpanel', function(err, db) {
        if (err) {
            throw err;
        }
        console.log("Connected to Database");

        //insert record
        db.collection('people').insert(people, function(err, records) {
            if (err) {
                throw err;
            }
            db.close();
        });
    });
}

var thisPage = 0;
var mpResult, thesePeople, cleanPeople;
var allPeople = [];

// need to hash and sort each new query string and splice in the new sig

function getPeople() {
    var sortedQs = sortByKey(peopleQueryParams);
    var mpSig = hashQueryString(sortedQs, apiSecret);
    options.qs = __.clone(peopleQueryParams);
    options.qs.sig = mpSig;
    options.url = peopleUrl;
    console.log('options: ', options);
    request(options, function (error, response, body) {
        if (!error && response.statusCode === 200) {
            console.log('thisPage: ', thisPage);
            console.log("Requested URL: ", response.request.uri.href);
            mpResult = JSON.parse(body);
            thesePeople = mpResult.results;
            peopleQueryParams.page = thisPage + 1;
            peopleQueryParams.session_id = mpResult.session_id;
            allPeople.push.apply(allPeople, thesePeople);
            console.log('thesePeople count: ', thesePeople.length);
            console.log('allpeopleCount: ', allPeople.length);
            if (thesePeople.length >= mpResult.page_size && thisPage < 100) {
                getPeople();
            } else {
                allPeople = removeDuplicateRecords(allPeople);
                cleanPeople = returnValidFields(allPeople);
                console.log("deduped people: ", allPeople.length); // Print the google web page.
                savePeople(cleanPeople);
            }
        } else {
            console.log('error! ', response);
        }
    });
}

function getEvent(event) {
    console.log('getting event: ', event);
    if(event) {
        eventQueryParams.event = event;
    }
    var sortedQs = sortByKey(eventQueryParams);
    var mpSig = hashQueryString(sortedQs, apiSecret);
    options.qs = __.clone(eventQueryParams);
    options.qs.sig = mpSig;
    options.url = eventUrl;
    console.log('options: ', options);
    request(options, function (error, response, body) {
        if (!error && response.statusCode === 200) {
            console.log('thisPage: ', thisPage);
            console.log("Requested URL: ", response.request.uri.href);
            // console.log('body: ', body);
            var strLines = body.split("\n");
            var thisJsonEvent;
            var thisDate;
            var d;
            var jsonEvents = [];
            for (var i in strLines) {
                if(strLines[i]) {
                    thisJsonEvent = JSON.parse(strLines[i]);
                    thisDate = thisJsonEvent.properties.time * 1000;
                    d = new Date(thisDate); // The 0 there is the key, which sets the date to the epoch    
                    thisJsonEvent.date = d;
                    jsonEvents.push(thisJsonEvent);
                }
            }
            //console.log(jsonEvents);
            var cleanEvents = returnValidPropertyFields(jsonEvents);
            saveEvents(cleanEvents);
        } else {
            console.log('error! ', response);
        }
    });    
}

if (userArgs[0] === "people") {
    getPeople();
} if (userArgs[0] === "event") {
    getEvent(userArgs[1]);
} else {
    console.log('Whoops, please specify what you want to extract, ie people or events');
}
