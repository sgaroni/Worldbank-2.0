#!/usr/local/bin/python3
# Copyright (c) 2019 DataEngineering.org
# All rights reserved.

# Developed by:	 Dr. Eric W. Davis
# 			 http://dataengineering.org
			 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal with the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimers in the documentation and/or other materials provided with the distribution.
# Neither the names of <Name of Development Group, Name of Institution>, nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.

# ARChive Module
import h5py
import collections
import os
import sys
import re
import json
import numpy as np
import gzip
from pathlib import Path
import shutil
import os

modulePath = os.path.dirname(__file__)

class DataError(Exception):
    """A class for generic data errors in the ARChive format.

    Triggered when trying to access a non-existant dataset, etc.
    """

    def __init__(self, value):
        """Initializes DataError Class with a string value"""
        self.value = value
    def __str__(self):
        """String operator overloading"""
        return repr(self.value)
    
class ARChive:
    """A class for holding our specialized format of HDF5
    """
    
    def __init__(self, filename, mode=None):
        """Initializes the ARChive class
        
        Args:
            filename: Mandatory name of the hdf5 record
            mode: Mode for opening.  Usually you'll want "w" to overwrite or build
                  a new file, and "r+" to open an existing file.
        """
        hivename = filename + ".hive"
        self.filename = filename
        self.hivename = hivename
        myFile = Path(hivename)
        if (myFile.is_file()):
            with gzip.open(hivename, 'rb') as f_in:
                with open(filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        if (mode is None):
            self.archive = h5py.File(filename)
        else:
            self.archive = h5py.File(filename, mode)
        self.version = "V0.1"
        self.refCode = "/ref"

    def initialize(self, template=modulePath + "/hive.template", base=""):
        """ Initializes the ARChive object

        This generally needs to be called before you can use the ARChive object.  It builds an empty
        structure according to the provided template.  If opening an already written ARChive file,
        this is unnecessary.

        Args:
            template: optional argument which points to the hive template to use.
                      hive template formate is documented elsewhere.  A default
                      template for the World Bank ARC project is included in the
                      module.
        """
        with open(template, "r") as fin:
            for line in fin:
                line = line.rstrip()
                if (re.match("^#.*",line)):
                    continue
                elif (re.match("^@.*",line)):
                    # Command
                    match = re.match("^@version(.*)",line)
                    if (match):
                        self.version = match.group(1)
                    continue
                elif (re.match(".*/\s*$", line)):
                    if (base != ""):
                        if (re.search(".*"+self.refCode+"(.*)", line)):
                            match = re.search(".*"+self.refCode+"(.*)", line)
                            line = base + match.group(1)
                        else:
                            continue
                    groupname = line[:-1]
                    self.archive.create_group(groupname)
                    #sys.stdout.write("G:")
                else:
                    if (base != ""):
                        if (re.search(".*"+self.refCode+"(.*)", line)):
                            match = re.search(".*"+self.refCode+"(.*)", line)
                            line = base + match.group(1)
                        else:
                            continue
                    datasetname = line
                    if (datasetname == "/records"):
                        myType = np.int32
                    else:
                        myType = h5py.special_dtype(vlen=str)
                    self.archive.create_dataset(datasetname, (1,), dtype=myType, chunks=(1,), compression="gzip")
                    #sys.stdout.write("D:")
                #print("$",line,"$")

        if (base == ""):
            self.setData("hiveversion", self.version)
            self.setData("records", 0)

    def addCell(self):
        currentRecords = int(self.archive["/records"].value)
        self.setData("/records", currentRecords+1)
        recordLabel = '/{:010d}'.format(currentRecords+1)
        self.archive.create_group(recordLabel)
        self.initialize(base=recordLabel)
        return(recordLabel)

    def getCells(self):
        totalRecords = self.getData('records')
        identifiers = []
        for idx in range(1, totalRecords+1):
            id = "{:010d}".format(idx)
            if (isinstance(self.archive[id], h5py.Group)):
                identifiers.append(self.archive[id])
            else:
                raise DataError(id + " not a valid group!  HDF5 corrupted.")
        return(identifiers)
        

    def inARChive(self, name, base=None):
        if (base is None):
            base = self.archive
        return(name in base)
                    
    def isGroup(self, groupname, base=None):
        if (base is None):
            base = self.archive
        groupname = base[groupname].name
        return((groupname in self.archive) and (isinstance(self.archive[groupname], h5py.Group)))

    def isData(self, dataname, base=None):
        if (base is None):
            base = self.archive
        dataname = base[dataname].name
        return((dataname in self.archive) and (isinstance(self.archive[dataname], h5py.Dataset)))

    def isEmpty(self, dataname, base=None):
        if (base is None):
            base = self.archive
        dataname = base[dataname].name
        if (not self.isData(dataname)):
            raise DataError(dataname + " is not a valid dataset.")
        if (isinstance(self.archive[dataname][0], np.int32)):
            return(False)
        return(len(self.archive[dataname][0]) == 0)

    def getData(self, dataname, base=None):
        if (base is None):
            base = self.archive
        dataname = base[dataname].name
        if (not self.isData(dataname)):
            raise DataError(dataname + " is not a valid dataset.")
        return(self.archive[dataname][0])

        
    def setData(self, dataname, value, base=None):
        if (base is None):
            base = self.archive
        dataname = base[dataname].name
        if (not self.isData(dataname)):
            raise DataError(dataname + " is not a valid dataset.")
        self.archive[dataname][0] = value
        
    def toDict(self, base=None):
        if (base is None):
            base = self.archive
        theDict = {}
        for name, obj in list(base.items()):
            if (isinstance(obj, h5py.Group)):
                theDict[name] = self.toDict(base=obj)
            elif (isinstance(obj, h5py.Dataset)):
                if self.isEmpty(name, base=base):
                    theDict[name] = ""
                else:
                    theDict[name] = str(obj.value[0])
        return(theDict)

    def flattenDict(self, d, parent_key='', sep='/'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else sep+k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flattenDict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return(dict(items))
        
    def toFlatDict(self, base=None, prefix="/"):
        if (base is None):
            base = self.archive
        myDict = self.toDict(base=base)
        return(self.flattenDict(myDict))
                    
    def toDoc(self, base=None):
        if (base is None):
            base = self.archive
        theDict = {}
        theDict["ARChive"] = self.toDict(base=base)
        return(theDict)

    def toFlatDoc(self, base=None):
        if (base is None):
            base = self.archive
        theDict = {}
        theDict["ARChive"] = self.toFlatDict(base=base)
        return(theDict)

    def toJSON(self, base=None):
        if (base is None):
            base = self.archive
        return(json.dumps(self.toDict(base=base), sort_keys=False))

    def dumpJSON(self, base=None, filename="output.json"):
        if (base is None):
            base = self.archive
        with open(filename, "w") as f:
            json.dump(self.toDict(base=base), f, sort_keys=False)
    
    def fromJSON(self, jsonObj):
        self.fromDict(json.loads(jsonObj))
        
    def fromDict(self, dictObj, prefix="/"):
        for (key, value) in dictObj.items():
            if (isinstance(value, dict)):
                self.fromDict(value, prefix+key+"/")
            else:
                self.setData(prefix+key, value)
        
    def printHierarchy(self, base=None, depth=0, prefix="/"):
        if (base is None):
            base = self.archive
        for name, obj in list(base.items()):
            for idx in range(0, depth):
                sys.stdout.write("\t")
            if isinstance(obj, h5py.Group):
                print("G:", prefix+name, "[" + str(len(obj.items())) + "]")
                self.printHierarchy(obj, depth=depth+1, prefix=prefix+name+"/")
            elif isinstance(obj, h5py.Dataset):
                if (name == "records"):
                    print("D:", name, ":", obj.value)
                else:
                    print("D:", name, ":", len(obj[0]), ":", obj.value)

    def getHierarchy(self, base=None, depth=0, prefix="/"):
        representation = "<ul>"
        if (base is None):
            base = self.archive
        for name, obj in list(base.items()):
            if isinstance(obj, h5py.Group):
                representation = representation + "<li>" + prefix+name + " (G)</li>"
                representation = representation + self.getHierarchy(obj, depth=depth+1, prefix=prefix+name+"/")
            elif isinstance(obj, h5py.Dataset):
                representation = representation + "<li>" + prefix+name + " (D)</li>"
        return(representation + "</ul>")

    def getHierarchyD(self, base=None, depth=0, prefix="/"):
        dataItems = []
        if (base is None):
            base = self.archive
        for name, obj in list(base.items()):
            if isinstance(obj, h5py.Group):
                dataItems = dataItems + self.getHierarchyD(obj, depth=depth+1, prefix=prefix+name+"/")
            elif isinstance(obj, h5py.Dataset):
                dataItems.append(prefix+name)

        return(dataItems)

    def getHierarchyG(self, base=None, depth=0, prefix="/"):
        if (prefix == "/"):
            representation = "<ul>"
        else:
            representation = ""
        if (base is None):
            base = self.archive
        for name, obj in list(base.items()):
            if isinstance(obj, h5py.Group):
                representation = representation + "<li>" + prefix+name + " (G)</li>"
                representation = representation + self.getHierarchyG(obj, depth=depth+1, prefix=prefix+name+"/")
        if (prefix == "/"):
            return(representation + "</ul>")
        else:
            return(representation)
        
    def close(self):
        self.archive.close()
        with open(self.filename, 'rb') as f_in:
            with gzip.open(self.hivename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(self.filename)
