import time

import luigi
import pandas as pd
import torch.nn.functional as F

from adapters import AutoAdapterModel
from torch import Tensor
from transformers import AutoTokenizer

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
        self.num_records_to_checkpoint = self.config.TEXT_EMBEDDING.NUM_RECORDS_TO_CHECKPOINT
        self.batch_size = self.config.TEXT_EMBEDDING.BATCH_SIZE
        # Load the model and tokenizer
        self.model = AutoAdapterModel.from_pretrained('allenai/specter2_base')
        self.model.load_adapter("allenai/specter2", source="hf", load_as="specter2", set_active=True)
        self.tokenizer = AutoTokenizer.from_pretrained('allenai/specter2_base')

        # Delete before execution
        self.delete_insert = False

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
            SELECT s.article_id,
                   s.article_title,
                   s.article_container_title,
                   s.article_abstract,
                   s.article_references
            FROM stg_article s
            LEFT JOIN article_text_embedding t 
                ON S.article_id = t.article_id
            WHERE t.article_id IS NULL
        """
        # Fetch the DOIs from the PostgreSQL database
        df = query(conn=self.pg_connection,
                   query=query_str)

        # To list of dictionaries
        articles = df.to_dict(orient='records')

        # Batch articles
        batched_articles = [articles[i:i + self.batch_size] for i in range(0, len(articles), self.batch_size)]

        # Log the number of batches
        self.logger.info(f"Number of article batches to embed: {len(batched_articles)}")

        # Batch the articles
        return batched_articles

    def process_item(self, item: list) -> list:
        """
        Process a single item
        :param item: DOI for Crossref article
        :return: Record with publication metadata
        """
        # Start timer
        start = time.time()

        # Get the articles
        document_batch = item
        document_batch_embedding_inputs = [
            self.tokenizer.sep_token.join([
                document['article_title'] or '',
                document['article_container_title'] or '',
                document['article_abstract'] or '',
                document['article_references'] or ''
            ]) for document in document_batch
        ]

        # Embed the articles
        embeddings = self.embed_batch(batch=document_batch_embedding_inputs)

        # Combine the articles with the embeddings
        for i, article in enumerate(document_batch):
            print(i)
            article['article_text_embedding'] = '{' + ','.join(map(str, embeddings[i].tolist())) + '}'
            # Pop article text key
            article.pop('article_title', None)
            article.pop('article_container_title', None)
            article.pop('article_abstract', None)
            article.pop('article_references', None)

        # Log the time taken
        self.logger.debug(f"Time taken to embed batch: {time.time() - start:.2f} seconds")
        return document_batch

    def to_dataframe(self, iterable: list) -> pd.DataFrame:
        """
        Convert the iterable to a DataFrame
        :param iterable: List of dictionaries
        :return: DataFrame
        """
        return pd.DataFrame(iterable)

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
