from crossref.restful import Works, Etiquette
import logging
from datetime import datetime
import time
import pandas as pd

logging.basicConfig(filename='get_papers.log', level=logging.INFO)

e = Etiquette('Authorship research project', 'V1.0', 'https://github.com/ilyaaltshteyn', 'ilyaaltshteyn@gmail.com')

w = Works(etiquette=e)
keys = [
    'DOI', 'ISSN', 'URL', 'author', 'container-title',
    'is-referenced-by-count', 'issn-type', 'issued', 'license',
    'link', 'published-online', 'published-print', 'publisher',
    'references-count', 'source', 'subject', 'title', 'type',
    ]
counter = 0
backoff = 0
data = {}
starttime=datetime.now()

for item in w.query():
    try:
        temp_dat = {}
        for k in keys:
            temp_dat[k] = item.get(k)
        data[item['DOI']] = temp_dat

        # Reduce backoff if nothing went wrong
        if backoff > 1:
            backoff -= 1

        # Write to flatfile and reset memory
        counter += 1
        if counter % 100 == 0:
            outdir = '/Users/ilya/code/random/metascience/data/crossref_{ix}.csv'
            df = pd.DataFrame.from_dict(data).transpose()
            df.to_csv(outdir.format(ix=counter/100), index=False)

            msg = 'Completed writing file {ix} at {tm}. Total write time={wt}'
            logging.info(msg.format(
                ix=counter/100,
                tm=datetime.now(),
                wt = datetime.now()-starttime
                )
            )

            # Reset for new file
            data = {}
            starttime=datetime.now()
        if counter % 10 == 0:
            print 'Completed :', counter

    except Exception, e:
        print str(e)
        logging.error(str(e) + ' at ' + str(datetime.now()) + ' with backoff at ' + str(backoff))
        backoff += 1
        time.sleep(backoff)
