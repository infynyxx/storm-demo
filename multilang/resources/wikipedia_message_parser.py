# -*- coding: utf-8 -*-

import storm
import re
try:
    import simplejson as json
except ImportError:
    import json

json_decode = lambda x: json.loads(x)

class WikipediaMessageParserBolt(storm.BasicBolt):

    def process(self, tup):
        msg = tup.values[0]

        #message format: {"channel":"#en.wikipedia","raw":"[[Special:Log/newusers]] create  * Bgwhyte *  new user account","time":1421957185818,"source":"rc-pmtpa"}
        json = json_decode(msg.encode("utf-8"))
        raw_msg = json["raw"]

        # debugging
        #with open("/tmp/wikipedia_topics.txt", "w") as myfile:
        #    myfile.write(raw_msg.encode("utf-8") + "\n")

        pattern = re.compile("\\[\[.*\]\]\s")
        matches = pattern.match(raw_msg.encode("utf-8"))
        if matches is not None:
            storm.emit([matches.group().strip().strip("[[").strip("]]")])

WikipediaMessageParserBolt().run()
