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
