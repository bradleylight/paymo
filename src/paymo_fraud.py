#python3
#program that provides multiple levels of payment fraud alerts
#insight data engineering coding challenge, William Light, 2016

import sys, csv

#initialize

TRUSTED = 'trusted'
UNVERIFIED = 'unverified'

infile1 = sys.argv[1]
infile2 = sys.argv[2]
outfile1 = sys.argv[3]
outfile2 = sys.argv[4]
outfile3 = sys.argv[5]

Nset1st = {}  #map to set of 1st degree "friends" for each person/node
#Nset2nd = {}  #map to set of 2nd degree "friends" for each person/node

i = 0

#define

def friend_2nd_degree(id1, id2):
    for id in Nset1st[id1]:
        if id in Nset1st[id2]:
            return True
    return False

def friend_4th_degree(id1, id2):
    id2_2nd_friends = set()
    for id in Nset1st[id2]:
        id2_2nd_friends.update(Nset1st[id])
    for idB in Nset1st[id1]:
        for idC in Nset1st[idB]:
            if idC in id2_2nd_friends:
                return True
    return False

#open batch data file (close batch data file after setting initial graph state)
#read from batch payments file, one line into memory at a time!
#skip header, parse line, process, update graph

frbatch = open(infile1, 'rU')
breader = csv.reader(frbatch)
#row = next(breader, None)  # skip the header, validity check automates this
for row in breader:
    i += 1
    if len(row) < 3:
        print(i, row)
        continue
    if len(row[0]) != 19:
        print(i, row)
        continue
    id1 = row[1].strip()
    id2 = row[2].strip()
    #update graphs (batch processing section)
    id1ind = id1 in Nset1st
    id2ind = id2 in Nset1st
    if not (id1ind or id2ind):
        #both id1 and id2 are new (and so not connected)
        Nset1st[id1] = set([id2])
        Nset1st[id2] = set([id1])
    elif not id1ind:
        #id1 is new, id2 is not new (and so is connected elsewhere)
        Nset1st[id1] = set([id2])
        Nset1st[id2].add(id1)
    elif not id2ind:
        #id1 is not new, id2 is new
        Nset1st[id1].add(id2)
        Nset1st[id2] = set([id1])
    else:
        #neither id1 nor id2 are new, both are connected elsewhere)
        Nset1st[id1].add(id2)
        Nset1st[id2].add(id1)

#testing
print("1st = ", len(Nset1st))

frbatch.close()

#open stream data files (close stream data files after main loop)

frstream = open(infile2, 'rU')
fw1 = open(outfile1, 'w')
fw2 = open(outfile2, 'w')
fw3 = open(outfile3, 'w')

#main stream data loop
#

sreader = csv.reader(frstream)
#row = next(sreader, None)  # skip the header, validity check automates this
for row in sreader:
    i += 1
    if len(row) < 3:
        print(i, row)
        continue
    if len(row[0]) != 19:
        print(i, row)
        continue
    id1 = row[1].strip()
    id2 = row[2].strip()
    #evaluate alert statuses before updating graph
    if id1 == id2:
        alert1 = TRUSTED 
        alert2 = TRUSTED 
        alert3 = TRUSTED
    else:
        id1ind = id1 in Nset1st
        id2ind = id2 in Nset1st
        if not (id1ind and id2ind):
            alert1 = UNVERIFIED
            alert2 = UNVERIFIED
            alert3 = UNVERIFIED
        else:
            if id2 in Nset1st[id1]: #1st degree, id1 knows id2
                alert1 = TRUSTED 
                alert2 = TRUSTED 
                alert3 = TRUSTED
            else:
                if friend_2nd_degree(id1, id2):
                    alert1 = UNVERIFIED
                    alert2 = TRUSTED 
                    alert3 = TRUSTED
                else:
                    if friend_4th_degree(id1, id2):
                        alert1 = UNVERIFIED
                        alert2 = UNVERIFIED 
                        alert3 = TRUSTED
                    else:
                        alert1 = UNVERIFIED
                        alert2 = UNVERIFIED 
                        alert3 = UNVERIFIED
    fw1.write('%s' % alert1 + "\n")
    fw2.write('%s' % alert2 + "\n")
    fw3.write('%s' % alert3 + "\n")
    #update graphs (stream processing section)
    id1ind = id1 in Nset1st
    id2ind = id2 in Nset1st
    if not (id1ind or id2ind):
        #both id1 and id2 are new (and so not connected)
        Nset1st[id1] = set([id2])
        Nset1st[id2] = set([id1])
    elif not id1ind:
        #id1 is new, id2 is not new (and so is connected elsewhere)
        Nset1st[id1] = set([id2])
        Nset1st[id2].add(id1)
    elif not id2ind:
        #id1 is not new, id2 is new
        Nset1st[id1].add(id2)
        Nset1st[id2] = set([id1])
    else:
        #neither id1 nor id2 are new, both are connected elsewhere)
        Nset1st[id1].add(id2)
        Nset1st[id2].add(id1)

fw3.close()
fw2.close()
fw1.close()
frstream.close()
