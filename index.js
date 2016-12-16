'use strict';
const XLSX = require('xlsx');
const fs = require('fs');
const mkdirp = require('mkdirp');
const jsdiff = require('diff');
const firebase = require('firebase');
const fsp = require('fs-promise');
const md5 = require('md5');
const path = require('path');
const gutil = require('gulp-util');
const through = require('through2');
const Promisebluebird = require('bluebird');

const PLUGIN_NAME = 'excelsheets2jsonandfirebase';

var fb, hashes = {}, logtofile;

const CONFIGBASE = {
    groupname: 'details',
    firebasewrite: false,
    filewrite: false
};

const getsheetconf = function (s, i) {
    let Sheetconf = function (con) {
        for (let c in con) {
            this[c] = con[c];
        }
    };

    Sheetconf.prototype = Object.create(i);
    Sheetconf.prototype.constructor = Sheetconf;
    return new Sheetconf(s);
}

class e2js {
    stream(options) {
        return through.obj(function (file, enc, cb) {
            if (file.isNull()) {
                this.push(file);
                return cb();
            }
            if (file.isStream()) {
                this.emit('error', new gutil.PluginError(PLUGIN_NAME, 'Streaming not supported'));
                return cb();
            }
            init(file.contents, options, file.path)
            .then((json) => {
                file.contents = new Buffer(JSON.stringify(json));
                file.path = file.path.replace('.xls', '.json');
                this.push(file);
                cb();
            }).catch((e) => {
                cb(new gutil.PluginError(PLUGIN_NAME, e.message));
            });
        })
    }

    file(file, options) {
        let getfilecont = Promisebluebird.coroutine(function*() {
            let contents = yield fsp.readFile(cleanpath(file));
            let results = yield init(contents, options, file);
            return results;
        });
        return getfilecont();
    }
}

module.exports = new e2js();

function init(contents, optionsin, file) {
    try {
        if(arguments.length <3){
            throw new Error ('contents, optionsin or file are not set');
        }
        if(typeof (arguments[0]) != 'object'){
            throw new Error ('contents is not set correctly');
        }
        if(typeof (arguments[1]) != 'object'){
            throw new Error ('options are not set correctly');
        }
        if(typeof (arguments[2]) != 'string'){

            throw new Error ('file is not set correctly');
        }
    } catch (e) {
        return Promise.reject(e);
    }
    let parseWorkbook = Promisebluebird.coroutine(function*() {
        let workbook = XLSX.read(contents);
        let optionspre = yield readoptions(workbook, optionsin);
        let options = yield finishoptions(workbook, optionspre, file);
        let results = toJson(workbook, options, optionsin);
        let toFtoF = yield toFiletoFirebase(results, options);
        return results;
    });
    return parseWorkbook();
}

/*
 function readoptions(workbook, options) {
 return new Promise((resolve, reject) => {
 if (typeof (options) === 'object') {
 resolve(options);
 } else if (workbook.SheetNames.includes(options)) {
 resolve(Object.create(getsheet(XLSX.utils.sheet_to_row_object_array(workbook.Sheets[options], {'header': 0}), CONFIGBASE, options)).content);
 } else {
 fsp.readJson(cleanpath(options), {encoding: 'utf8'}).then((options) => {
 resolve(options);
 });
 }
 });
 }
 */

function readoptions(workbook, options) {
    try {
        if(arguments.length <2){
           throw new Error ('workbook or options are not set');
        }
        if(typeof (arguments[0]) != 'object'){
           throw new Error ('workbook is not set correctly');
        }
        if(typeof (arguments[1]) != 'object' && typeof (arguments[1]) != 'string'){
            throw new Error ('options are not set correctly');
        }
        if (typeof (options) === 'object') {
            return Promise.resolve(options);
        } else if (workbook.SheetNames.includes(options)) {
            return getsheetopts(workbook, options);
        } else {
            return getjsonopts(options);
        }
    } catch (e) {
        return Promise.reject(e);
    }
}

function getsheetopts(workbook, options) {
    try {
        if(arguments.length <2){
            throw new Error ('workbook or options are not set');
        }
        if(typeof (arguments[0]) != 'object'){
            throw new Error ('workbook is not set correctly');
        }
        if(typeof (arguments[1]) != 'object' && typeof (arguments[1]) != 'string'){
            throw new Error ('options are not set correctly');
        }

        let sheet = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[options], {'header': 0});
        let sheetval = getsheet(sheet, CONFIGBASE, options);
        let opts = Object.create(sheetval);
        let optcont = opts.content;

        console.log(optcont)
        return Promise.resolve(optcont);
    } catch (e) {
        return Promise.reject(e);
    }
}

function getjsonopts(options) {
    try {
        let file = cleanpath(options);
        let opts = fsp.readJson(file, {encoding: 'utf8'});
        return opts;
    } catch (e) {
        return Promise.reject(e);
    }
}

function finishoptions(workbook, o, file) {
    return new Promise((resolve, reject) => {
        try {
            let tempoptions = {};
            let options = Object.assign({}, CONFIGBASE, o);
            logtofile = options.logto ? options.logto : false;

            logex2js('Read', file);

            if (options.firebasedatabaseURL && options.firebaseserviceAccount) {
                if (typeof (fb) === 'undefined' || options.firebasedatabaseURL != fb.v.databaseURL) {
                    fb = initfirebase(options.firebasedatabaseURL, cleanpath(options.firebaseserviceAccount), hash(options.firebasedatabaseURL));
                    if (options.logOnALL || options.firebase.logOnALL) {
                        logex2js('firebase init', fb.v.databaseURL)
                    }
                }
                if (!options.firebasewrite != false) {
                    options.firebasewrite = true;
                }
            }
            if (options.filedestfolder && !options.filewrite != false) {
                options.filewrite = true;
            }
            if (!options.hasOwnProperty('sheets')) {
                options.sheets = {};
            }
            for (let k in options) {
                if (k != 'firebasedatabaseURL' && k != 'firebaseserviceAccount' && k != 'sheets') {
                    tempoptions[k] = options[k];
                }
            }
            workbook.SheetNames.forEach(function (sheetName) {
                if (!options.sheets.hasOwnProperty(sheetName)) {
                    options.sheets[sheetName] = {};
                }
                let roa = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[sheetName], {'header': 0});
                let c = roa.filter(getval, '##config##');
                if (c.length) {
                    try {
                        let conf = JSON.parse(c[0].ovalue);
                        conf.skiprow = c[0].index;
                        options.sheets[sheetName] = getsheetconf(Object.assign({}, tempoptions, options.sheets[sheetName], conf), tempoptions);
                    } catch (e) {
                        throw new Error(e + ' ' + sheetName + ' ##config## is no valid Json\n' + c[0].ovalue + '\n')
                    }
                } else {
                    options.sheets[sheetName] = getsheetconf(options.sheets[sheetName], tempoptions);
                }
                c = roa.filter(getval, '##datasbeginn##');
                if (c.length) {
                    options.sheets[sheetName].hasdata = true;
                }
            });
            resolve(options);
        } catch (e) {
            reject(e);
        }
    });
}

function toJson(workbook, options) {
    let results = {};
    workbook.SheetNames.forEach(function (sheetName) {
        let roa = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[sheetName], {'header': 0});
        if (roa.length) {
            let cursheet = getsheet(roa, options.sheets[sheetName], sheetName);
            results[cursheet.sheetname] = cursheet;
        }
    });
    return results;
}

function getsheet(roa, options, s) {

    try {
        let cursheet = {};
        cursheet.sheetname = s;
        if (options.hasOwnProperty('skiprow')) {
            roa.splice(options.skiprow, 1);
        }

        if (!options.hasdata) {
            cursheet.content = {};
            for (let i = 0; i < roa.length; i++) {
                if (typeof roa[i].oname != 'undefined') {
                    cursheet.content[roa[i].oname] = getparseval(roa[i].ovalue);
                }
            }
        } else {
            let g = cursheet.groupname;
            cursheet.content = {};
            cursheet.content[g] = {};
            let detsarr = [];
            let detsarr2 = [];
            for (var i = 0; i < roa.length; i++) {
                if (typeof roa[i].oname != 'undefined') {
                    if (roa[i].oname === '##datasbeginn##') break;
                    cursheet.content[roa[i].oname] = getparseval(roa[i].ovalue);
                }
            }
            for (let de in roa[i]) {
                if (de != 'oname' && de != 'index') {
                    cursheet.content[g][roa[i][de]] = {};
                    detsarr.push(de);
                    detsarr2.push(roa[i][de]);
                }
            }
            for (let j = i + 1; i < roa.length; j++) {
                if (typeof roa[i].oname != 'undefined') {
                    if (roa[j].oname === '##datasend##') break;
                    for (let d = 0; d < detsarr.length; d++) {
                        cursheet.content[g][detsarr2[d]][roa[j].oname] = getparseval(roa[j][detsarr[d]]);
                    }
                }
            }
        }

        return cursheet;
        //console.log(cursheet);
    } catch (e) {
        throw e //new Error(e)
    }
}

function toFiletoFirebase(sheets, options) {
    return new Promise((resolveall, rejectall) => {
        let tofile = [], tofb = [], toall = [];
        for (let sheet in sheets) {
            if (options.sheets[sheet].filewrite && !options.sheets[sheet].silent) {
                tofile.push(updatefile(sheets[sheet], options.sheets[sheet]));
            }
            if (options.sheets[sheet].firebasewrite) {
                tofb.push(updatefirebase(sheets[sheet], options.sheets[sheet]));
            }
        }

        let tofile2 = Promise.all(tofile)
        .then((p) => {
            logex2js('write-all');
            Promise.resolve();
        }).catch((e)=> {
            reject(e);
        });

        let tofb2 = Promise.all(tofb)
        .then((p) => {
            logex2js('write-all-remote');
            Promise.resolve();
        }).catch((e)=> {
            reject(e);
        });

        toall.push(tofile2);
        toall.push(tofb2);

        Promise.all(toall)
        .then((p) => {
            logex2js('all');
            resolveall();
        }).catch((e)=> {
            rejectall(e);
        });
    });

    resolve(sheets);
}

function updatefile(sheet, options) {
    if (!options.filename) options.filename = sheet.sheetname;
    if (!options.file) options.file = cleanpath(path.join(options.filedestfolder, options.filename + ".json"));

    return new Promise((resolve, reject) => {
        isnewfile(options.file, sheet.content).then((r) => {
            if (r) {
                fsp.writeFile(options.file, JSON.stringify(sheet.content))
                .then(function () {
                    if (options.logonsingle || options.filelogonsingle) {
                        logex2js('write', sheet.sheetname, options.file);
                    }
                    resolve();
                })
                .catch((err) => {
                    reject(err, 'Cant write File ' + options.file);
                });
            } else {
                if (options.logonsingle || options.filelogonsingle) {
                    logex2js('exists2', sheet.sheetname, options.file);
                    resolve();
                }
            }
        });
    });
}

function updatefirebase(sheet, options) {
    return new Promise((resolve, reject) => {
        if (!options.fbref) options.fbref = sheet.sheetname;
        let curhash = hash(sheet.content);
        if (isnewfb(options.fbref, curhash)) {
            var db = fb.database();
            var curRef = db.ref(options.fbref);
            switch ('push') {
                case 'set':
                    var dbref = curRef.set(sheet.content);
                    break;
                case 'update':
                    var dbref = curRef.update(sheet.content);
                    break;
                case 'push':
                    var dbref = curRef.push(sheet.content);
            }
            dbref.then(function () {
                if (options.logonsingle || options.firebaselogonsingle) {
                    logex2js('write-remote', sheet.sheetname, options.fbref);
                }
                hashes[options.fbref] = curhash;
                resolve();
            }).catch(function (error) {
                logex2js('write-remote failed', error, sheet.sheetname, options.fbref);
                throw new error('write-remote failed', error, sheet.sheetname, options.fbref);
                reject('write-remote failed', error, sheet.sheetname, options.fbref);
            });

        } else {
            if (options.logonsingle || options.firebaselogonsingle) {
                logex2js('exists-remote', sheet.sheetname, options.fbref);
            }
            resolve();
        }
    });
}

function isnewfile(file, content) {
    return new Promise(function (resolve) {
        fsp.readFile(file, 'utf8')
        .catch(() => {
            resolve(true);
        })
        .then((o) => {
            if (jsdiff.diffJson(JSON.parse(o), content).length > 1) {
                resolve(true);
            } else {
                resolve(false);
            }
        });
    });
}

function isnewfb(ref, ha) {
    if (hashes.hasOwnProperty(ref)) {
        if (hashes[ref] != ha) {
            return true;
        }
    } else {
        return true;
    }
}

function getval(v, i) {
    if (v.oname === this) {
        v.index = i;
        return v;
    }
}

function getparseval(v) {
    try {
        return JSON.parse(v);
    } catch (err) {
        return v;
    }
}

function makecallback(cb, data) {
    //let mcb = eval(cb+'('+data+')');
    //console.log(global);
    //let pp = global[cb](data);
    try {
        // console.log(module.parent.global['mycb2']('sds')); // this prints 'hi'

    } catch (e) {
        console.log(e);
    }
    /*
     new Promise(() => {
     if (typeof (global[cb]) === 'function') {
     global[cb](data);
     } else {
     logex2js(cb, 'seems not to be a function');
     }
     });
     return 'dada';
     */
}

function hash(v) {
    return md5(JSON.stringify(v));
}

function initfirebase(databaseURL, serviceAccount, name) {
    return firebase.initializeApp({
        databaseURL: databaseURL,
        serviceAccount: serviceAccount
    }, name);
}

function cleanpath(p) {
    if (!path.isAbsolute(p)) {
        return path.normalize(path.join(path.dirname(module.parent.filename), p))
    } else {
        return p;
    }
}

function logex2js(...args) {
    args.unshift(new Date().toLocaleString());
    if (logtofile) {
        fsp.appendFile(logtofile, args.map(getstringval).join('\t') + '\n')
        .catch((err) => {
            console.log(err, 'Cant write File ' + logtofile);
        });
    } else {
        console.log(args.join(' '));
    }
}

function getstringval(v) {
    if (typeof v === 'object') {
        return JSON.stringify(v);
    } else {
        return v;
    }
}
