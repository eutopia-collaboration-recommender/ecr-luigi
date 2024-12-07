import time

import luigi
from torch import Tensor
from transformers import AutoModel, AutoTokenizer
import torch.nn.functional as F

from util.embedding import average_pool
from util.luigi.eutopia_task import EutopiaTask
from util.common import to_snake_case
from util.postgres import query


class EmbedArticlesTask(EutopiaTask):
    """
    Description: A Luigi task to embed the text of articles in the PostgreSQL database using a transformer model.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_target_table_name = 'article_text_embedding'
        self.pg_target_schema = self.config.POSTGRES.DBT_SCHEMA
        self.pg_source_schema = self.config.POSTGRES.DBT_SCHEMA
        self.batch_size = self.config.TEXT_EMBEDDING.BATCH_SIZE
        # Load the model and tokenizer
        self.model = AutoModel.from_pretrained(self.config.TEXT_EMBEDDING.MODEL)
        self.tokenizer = AutoTokenizer.from_pretrained(self.config.TEXT_EMBEDDING.MODEL)

        # Delete before execution
        self.delete_insert = True

    updated_date_start: str = luigi.OptionalParameter(
        description='Search start date',
        default=time.strftime("%Y-%m-%d",
                              time.gmtime(time.time() - 7 * 24 * 60 * 60))
    )

    updated_date_end: str = luigi.OptionalParameter(
        description='Search end date',
        default=time.strftime("%Y-%m-%d", time.gmtime(time.time())))

    def embed_batch(self, batch: list) -> Tensor:
        """
        Embed a batch of input texts using a transformer model.
        :param batch: List of articles
        :return: List of embeddings
        """
        # Tokenize the input texts
        batch_dict = self.tokenizer(batch, max_length=512, padding=True, truncation=True, return_tensors='pt')

        # Get the embeddings from the model
        outputs = self.model(**batch_dict)
        # Average pool the embeddings
        embeddings = average_pool(outputs.last_hidden_state, batch_dict['attention_mask'])

        # Normalize embeddings
        normalized_embeddings = F.normalize(embeddings, p=2, dim=1)

        # Return the normalized embeddings
        return normalized_embeddings

    def query_records_to_update(self) -> list:
        """
        Query the articles and corresponding texts that will be embedded from the PostgreSQL database
        :return: List of DOIs
        """
        query_str = f"""
            SELECT DISTINCT article_doi   AS article_id,
                            article_title AS article_text
            FROM stg_crossref_publication s
            LEFT JOIN article_text_embedding t 
                ON S.article_doi = t.article_id
            WHERE t.article_id IS NULL
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)

        # To list of dictionaries
        articles = df.to_dict(orient='records')
        # Batch the articles
        return [articles[i:i + self.batch_size] for i in range(0, len(articles), self.batch_size)]

    def process_item(self, item: list) -> list:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        batch_articles = item
        batch = [article['article_text'] for article in batch_articles]

        # Embed the articles
        embeddings = self.embed_batch(batch=batch)

        # Combine the articles with the embeddings
        for i, article in enumerate(batch_articles):
            article['article_text_embedding'] = '{' + ','.join(map(str, embeddings[i].tolist())) + '}'
            # Pop article text key
            article.pop('article_text', None)

        return batch_articles

    def output(self):
        """
        Output target for the task used to check if the task has been completed.
        """
        updated_date_start = to_snake_case(self.updated_date_start)
        updated_date_end = to_snake_case(self.updated_date_end)
        target_name = f"embed_articles_{updated_date_start}_{updated_date_end}"
        return luigi.LocalTarget(f"out/{target_name}.json")


if __name__ == '__main__':
    luigi.build([
        EmbedArticlesTask(),
    ], local_scheduler=True)
