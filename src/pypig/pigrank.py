#!/usr/bin/python
from org.apache.pig.scripting import Pig

# NE PAS CHANGER LE PLACE HOLDER COMME EN PIG ON PEUT PAS METTRE D'ARGUMENTS COMME EN SPARK DIRECTEMENT
# ILS SONT MODIFIES AFIN DE CREER UN SCRIPT TEMPORAIRE LES CONTENANT QUI PERMETTRONT DE FAIRE TOURNER LEXPERIMENT
# voir le run.sh !
bucket = 'gs://PLACEHOLDER_BUCKET'
nb_iter = int('PLACEHOLDER_ITER')
input_file = 'PLACEHOLDER_INPUTFILE'

def run(script, params, new_out):
    params['INPUT'] = params['OUTPUT']
    params['OUTPUT'] = bucket + '/out/pig/' + new_out
    stats = script.bind(params).runSingle()
    if not stats.isSuccessful():
        print('failed script: ' + str(script) + '\nwith config: ' + str(params))
        exit(1)

if __name__ == "__main__":
    pig_init = Pig.compile('''
        data =
            LOAD '$INPUT'
            USING PigStorage(' ')
            AS ( url: chararray, p: chararray, link: chararray );

        data_distinct =
            DISTINCT data;

        url_data =
            GROUP data_distinct
            BY url;

        links =
            FOREACH url_data
            GENERATE
                group AS url,
                data_distinct.link AS links;

        new_pagerank =
            FOREACH url_data
            GENERATE
                group AS url,
                1 AS pagerank;

        STORE links
            INTO '$LINKS'
            USING PigStorage('\t');

        STORE new_pagerank
            INTO '$OUTPUT'
            USING PigStorage('\t');
        ''')
    pig_update = Pig.compile('''
        -- PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))

        previous_pagerank =
            LOAD '$INPUT'
            USING PigStorage('\t')
            AS ( url: chararray, pagerank: float );

        links =
            LOAD '$LINKS'
            USING PigStorage('\t')
            AS ( url: chararray, links: { link: ( url: chararray ) } );

        outbound_pagerank =
            FOREACH
                ( JOIN previous_pagerank BY url RIGHT, links BY url )
            GENERATE
                ( pagerank IS NULL ? 1 - $d : pagerank) / COUNT ( links ) AS pagerank,
                FLATTEN ( links ) AS to_url;

        new_pagerank =
            FOREACH
                ( GROUP outbound_pagerank BY to_url )
            GENERATE
                group AS url,
                ( 1 - $d ) + ( SUM( outbound_pagerank.pagerank ) IS NULL ? 0 : $d * SUM( outbound_pagerank.pagerank ) ) AS pagerank;

        STORE new_pagerank
            INTO '$OUTPUT'
            USING PigStorage('\t');
        ''')
    pig_max = Pig.compile('''
        previous_pagerank =
            LOAD '$INPUT'
            USING PigStorage('\t')
            AS ( url: chararray, pagerank: float );

        pagerank_sorted =
            ORDER previous_pagerank
            BY pagerank DESC;

        max_pagerank =
            LIMIT pagerank_sorted 1;

        STORE max_pagerank
            INTO '$OUTPUT'
            USING PigStorage('\t');
        ''')

    params = { 'd': '0.85', 'LINKS': bucket + '/out/pig/pagerank_links', 'OUTPUT': input_file }
    run(pig_init, params, 'pagerank_data_0')

    for i in range(1, nb_iter + 1):
        run(pig_update, params, 'pagerank_data_' + str(i))

    run(pig_max, params, 'pagerank_max')