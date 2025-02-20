- **event2018_news.tsv** : 18,098 articles. 
    - text
    - title
    - id reference to ot_media
    - label is event_id (**human!?**) or could be extended = if one article with label gold is in a cluster, giving this label to the cluster. (description a plusieurs docs pour un label)
    - pred is **automatic**
    event2018_news.csv contient des titres et textes d'articles, sur une periode qui recouvre celle du dataset de tweets.
    Des articles automatiquement associés à des événements, dont les clusters ont été reliés à un label selon si un article du corpus gold se trouvait dedans.
- **description.tsv**: description of the events. Des articles, détectés et attachés à des événements par des annotateur.ices. Un corpus “gold”.
    - event: id of the event. The first 8 digits give the date of the
    event
    - label: label of the event, allowing to join this file with
    relevant_tweets.tsv
    - label_day: label of the daily event (events lasting several
    days have a different label_day for each day)
    - otmedia_doc_id: id of the press article linked to the event,
    allowing to join this file with linked_articles.tsv. **There is 4 <empty> in this column.**

Il y a eu deux tirage aléatoire : le premier pour 6 personnes (300 articles chacun.e) et le second pour 1 personne (600 articles).
Il y a 100 article en commun pour tous.tes les annotateur.ices, destiné à calculer un score inter-annoteur.