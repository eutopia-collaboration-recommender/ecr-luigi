import numpy as np
from torch import Tensor


def average_pool(last_hidden_states: Tensor,
                 attention_mask: Tensor) -> Tensor:
    """
    Average pooling of the last hidden states of a transformer model.
    :param last_hidden_states: The last hidden states of a transformer model.
    :param attention_mask: The attention mask of the input.
    :return: The average pooled embeddings.
    """
    # Mask the last hidden states
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    # Sum the last hidden states and divide by the number of non-padded tokens
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]


def cosine_similarity(vector: np.ndarray,
                      matrix: np.ndarray) -> float:
    """
    Calculate the cosine similarity between a vector and a matrix.
    :param vector: Numpy vector
    :param matrix: Numpy matrix
    :return: Cosine similarity between the vector and the matrix
    """

    # Calculate the dot product between the vector and each row of the matrix
    dot_product = np.dot(matrix, vector.T).flatten()

    # Calculate the norm of the matrix and the vector
    matrix_norms = np.linalg.norm(matrix, axis=1)
    vector_norm = np.linalg.norm(vector)

    # Calculate the cosine similarity
    cosine_similarities = dot_product / (matrix_norms * vector_norm)

    return cosine_similarities
