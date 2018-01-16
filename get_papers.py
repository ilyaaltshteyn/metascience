from crossref.restful import Works, Etiquette
import logging
from datetime import datetime
import time
import pandas as pd
import sys
import os
import traceback

logging.basicConfig(filename='get_papers.log', level=logging.INFO)

start_year = sys.argv[1]
years_range = range(int(start_year), 2018)

e = Etiquette('Authorship research project', 'V1.0', 'https://github.com/ilyaaltshteyn', 'ilyaaltshteyn@gmail.com')

w = Works(etiquette=e)
keys = [
    'DOI', 'ISSN', 'URL', 'author', 'container-title',
    'is-referenced-by-count', 'issn-type', 'issued', 'license',
    'link', 'published-online', 'published-print', 'publisher',
    'references-count', 'source', 'subject', 'title', 'type',
    ]

def run_q(year, data, starttime, counter, batch_size):
    """ year -- year to run query for. """

    backoff = 0

    q = w.query().filter(
        from_print_pub_date=str(year),
        until_print_pub_date=str(year)
    )
    for item in q:
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
            if counter % batch_size == 0:
                outdir = '/Users/ilya/code/random/metascience/data/crossref_{ix}.csv'
                df = pd.DataFrame.from_dict(data).transpose()
                df.to_csv(outdir.format(ix=counter/batch_size), index=False,
                    encoding='utf-8')

                msg = 'Completed writing file {ix} at {tm}. Total write time={wt}'
                logging.info(msg.format(
                    ix=counter/batch_size,
                    tm=datetime.now(),
                    wt = datetime.now()-starttime
                    )
                )

                # Reset for new file
                data = {}
                starttime=datetime.now()

            if counter % batch_size/4. == 0:
                print 'Completed :', counter

        except Exception, e:
            print str(e)
            logging.error(str(e) + str(traceback.print_exc()) + ' at ' + str(datetime.now()) + ' with backoff at ' + str(backoff))
            backoff += 1
            time.sleep(backoff)

    return {'data' : data,
        'starttime' : starttime,
        'counter' : counter,
        'batch_size' : batch_size
    }

batch_size = 5000
used_counters = [0]
for f in os.listdir('/Users/ilya/code/random/metascience/data/'):
    used_counters.append(f.split('_')[-1])

init_dat = {'data' : {},
    'starttime' : datetime.now(),
    'counter' : max(used_counters)*batch_size,
    'batch_size' : batch_size
}
for y in years_range:
    print 'WORKING ON YEAR ', y
    init_dat = run_q(y, **init_dat)
