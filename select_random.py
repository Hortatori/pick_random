import pandas as pd
import csv

"""
* open event2018_news and descirption.tsv
* on event2018_news.tsv
* filter out the press papers previously picked as events
(to note : some papers has no label, they were in cluster which didnt contain a labelled paper)
- event2018_news.tsv contains paper from 07.2018 to 08.2018, augmented with text.
* each label in descrption can have several documents => how to create a reference to compare? => just one random in description) 
"""

def load_description(filename):
    df_description = pd.read_csv(filename,
                                 sep = "\t",
                                 quoting=csv.QUOTE_ALL,
                                 )
    # 4 lignes sans id_otmedia pour la presse dans description.tsv. => dropna
    df_description = df_description.dropna(subset=["otmedia_doc_id"])
    # finally not dropping twitter issuers
    # df_description = df_description[~df_description['issuer'].str.contains("TWITTER")]
    return df_description

def load_news(filename):
    df_news = pd.read_csv(filename,
                          sep = "\t",
                          quoting=csv.QUOTE_ALL)
    return df_news

    
def random_docs(description):
    df_by_one = description.groupby("label").sample(n=1, random_state=1)
    return df_by_one


def detect_nan_values(df):
    nan_values = df.isnull().sum()
    print("sum of nan values",nan_values)
    nan_columns = nan_values[nan_values > 0].index
    print(f"NaN values in columns: {nan_columns}")
    print(df[nan_columns].head())

def check_up_labels_shape(df_description, df_filtered) :
    for i in pd.unique(df_filtered['label']) :
        if i not in pd.unique(df_description['label']) :
            print("label only in df_filtered", df_filtered[df_filtered['label'] == i].shape)
    for i in pd.unique(df_description['label']) :
        if i not in pd.unique(df_filtered['label']) :
            print("label only in descripion ", df_description[df_description['label'] == i].shape)


def sample_randomly(final) :
    final = final[['label','title','text','representative_title','representative_text']]
    final = final.sample(frac= 1, random_state=1)
    final['text'] = final['text'].str.replace('\n', ' ')
    return final

def extract_sampling(final, output_names):
    start = 0
    end = 100
    commun_100 = final[start:end]
    commun_100.to_csv("100_common_events.csv", quoting=csv.QUOTE_ALL, index=True)

    start += 100
    for name_out in output_names:
        end += 200
        print(f"start {start}, end {end}")
        df = final[start:end]
        df = pd.concat([df,commun_100], axis = 0, ignore_index=True)
        df = df.sort_values(by=['label'])
        # clean \n in text
        df['text'] = df['text'].str.replace('\n', ' ')
        df.to_csv(name_out, quoting=csv.QUOTE_ALL, index=True)
        print(f"df {df.shape}, name out {name_out}")
        start += 200

def main():
    df_description = load_description("description.tsv")
    df_news = load_news("event2018_news.tsv")

    # sample randomly one representative article for each label on df_description
    description_selection = random_docs(df_description)

    # filter identical docs and sample random in df_news
    common_docs = df_news['id'].isin(df_description['otmedia_doc_id'])
    df_filtered = df_news[~common_docs]
    df_filtered = df_filtered.drop_duplicates(subset=['title'], keep='first')
    df_filtered = df_filtered.drop_duplicates(subset=['text'], keep='first')

    check_up_labels_shape(df_description, df_filtered)


    output_names = ["antoine.csv", "beatrice.csv", "fabien.csv", "marjolaine.csv", "julien.csv", "qi.csv", "frederique.csv", "thierry.csv"]
    final = df_filtered.sample(n = len(output_names)*200+100, random_state = 1) # useless sample but stays for reproducibility w already extracted corpus, same for antoine.csv
    # rename column and choosing only the useful columns for clearer annotation :
    description_selection.rename(columns={"title" : "representative_title", "description" : "representative_text"}, inplace=True)


    # remove labels with only one article NOPE
    for i, j in description_selection.groupby("label") :
        if j.shape[0] > 1 :
            print(f"i {i}, j shape{j.shape}")

    # note : some labels do not exists in news and exist only in description (events appeared only once, in the first drawn in news/twitter)
    # merging to exclude these labels
    final = final.reset_index().merge(description_selection[['label','representative_title','representative_text']], on='label')
    unique1 = len(pd.unique(final['label']))
    print(f"post concat shape {final.shape}, labels {unique1}")
    final = sample_randomly(final)
    extract_sampling(final, output_names)

def read_and_concat(file_names) :
    global_concat = pd.DataFrame()
    for doc in file_names:
        temp = pd.read_csv(doc, quoting=csv.QUOTE_ALL)
        print(f"read {doc}, shape is {temp.shape}")
        global_concat = pd.concat([global_concat, temp], axis = 0)
    return global_concat

def main_second_sample() :
    df_description = load_description("description.tsv")
    df_news = load_news("event2018_news.tsv")

    # sample randomly one representative article for each label on df_description
    description_selection = random_docs(df_description)

    # masking out df_description docs from df_news and randomize in df_news
    common_docs = df_news['id'].isin(df_description['otmedia_doc_id'])
    df_filtered = df_news[~common_docs]
    # print(df_news["label"].value_counts(sort=True))
    df_filtered = df_filtered.drop_duplicates(subset=['title'], keep='first')
    df_filtered = df_filtered.drop_duplicates(subset=['text'], keep='first')

    # check_up_labels_shape(df_description, df_filtered)

    # rename column and choosing only the useful columns for clearer annotation :
    description_selection.rename(columns={"title" : "representative_title", "description" : "representative_text"}, inplace=True)


    # remove labels with only one article
    for label, count in df_filtered["label"].value_counts(sort=True).items():
        if count == 1 :
            df_filtered = df_filtered[df_filtered["label"] != label]
            description_selection = description_selection[description_selection["label"] != label]

    final = df_filtered.reset_index().merge(description_selection[['label','representative_title','representative_text']], on='label')
    # final = sample_randomly(final)
    final = final[['label','title','text','representative_title','representative_text']]

    file_names = ["beatrice.csv", "fabien.csv", "marjolaine.csv", "julien.csv", "qi.csv", "frederique.csv", "thierry.csv"]
    previous_subset = read_and_concat(file_names)
    previous_subset = previous_subset[['label','title','text','representative_title','representative_text']]
    # previous_subset = previous_subset.drop_duplicates(subset=['text'], keep=False) # drop les 100, mais peut etre fait sur le drop suivant

    #1
    print(f"before combined previous subset {previous_subset.shape} and header {list(previous_subset.columns)}, final {final.shape}, header : {list(final.columns)}")
    combined = pd.concat([final, previous_subset])
    combined['text'] = combined['text'].str.replace('\n', ' ')

    combined = combined.drop_duplicates(subset=['text'], keep=False) # drop les 200*7
    print(f"after combined combined {combined.shape}, header : {list(combined.columns)}, should be equal to {final.shape[0] - ((len(file_names)*200)+100)}")
    # combined = combined.drop_duplicates(subset=['title'], keep=False) # drop les 200*7
    #2
    # mask = final["text"].astype(str).isin(previous_subset["text"].astype(str))
    # combined = final[~mask]
    #3
    # Fusion avec outer join
    # merged = pd.merge(previous_subset, final, on='text', how='outer', indicator=True)
    # combined = merged[merged['_merge'] == 'right_only']


    print(f"previous subset {previous_subset.shape} and header {list(previous_subset.columns)}, final {final.shape}, header : {list(final.columns)} combined shape {combined.shape} (and headers {list(combined.columns)}), combined shape should equal {final.shape[0] - ((len(file_names)*200)+100)}")
    # les dimensions ne tombent pas juste et je ne comprends pas pourquoi
    # pour réduire le risque de répeter le même tirage, je commence à l'index 200*7 + 100, pcq les tirage sont reproductibles
    temp_100 = pd.read_csv("100_common_events.csv", quoting=csv.QUOTE_ALL)
    temp_100 = temp_100[['label','title','text','representative_title','representative_text']]


    # print("100 common event\n",temp_100)
    # print("output before concat with 100 common events\n", combined)
    combined = sample_randomly(combined)
    start = 200*7+100
    end = start+500
    first_output = combined[start:end]
    first_output = pd.concat([temp_100, first_output], axis = 0, ignore_index=True)
    first_output = first_output.sort_values(by=['label'])
    first_output.to_csv("francesca_600_sample.csv", quoting=csv.QUOTE_ALL, index=True)

    start = end
    end += 1000
    second_output = combined[start:end]
    second_output = pd.concat([temp_100, second_output], axis = 0, ignore_index=True)
    second_output = second_output.sort_values(by=['label'])
    second_output.to_csv("francesca_1100_sample.csv", quoting=csv.QUOTE_ALL, index=True)


#main()
# main_second_sample()