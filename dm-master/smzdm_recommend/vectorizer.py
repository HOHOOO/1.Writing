# coding=utf-8
import array
from collections import defaultdict, Mapping

import re
# import jieba
# jieba.initialize()
import numpy as np
import scipy.sparse as sp

from numpy import frombuffer as frombuffer_empty
from tools.stop_words import SIGNAL_STOP_WORDS, ENGLISH_STOP_WORDS

__all__ = ['CountVectorizer', 'MyCountVectorizer']


class VectorizerMixin(object):
    """Provides common code for text vectorizers (tokenization logic)."""

    _white_spaces = re.compile(r"\s\s+")

    def build_preprocessor(self):
        """Return a function to preprocess the text before tokenization"""
        if self.preprocessor is not None:
            return self.preprocessor

        if self.lowercase:
            return lambda x: x.lower()
        else:
            return lambda x: x

    def build_tokenizer(self):
        """Return a function that splits a string into a sequence of tokens"""
        if self.tokenizer is not None:
            return self.tokenizer
        token_pattern = re.compile(self.token_pattern)
        return lambda doc: token_pattern.findall(doc)

    def get_stop_words(self):
        """Build or fetch the effective stop words list"""
        return _check_stop_list(self.stop_words)

    def build_analyzer(self):
        """Return a callable that handles preprocessing and tokenization"""
        if callable(self.analyzer):
            return self.analyzer

        preprocess = self.build_preprocessor()

        if self.analyzer == 'char':
            return lambda doc: [char for char in preprocess(doc)]

        elif self.analyzer == 'word':
            stop_words = self.get_stop_words()
            tokenize = self.build_tokenizer()

            return lambda doc: [w for w in tokenize(preprocess(doc)) if w not in stop_words] if stop_words else tokenize(preprocess(doc))
        else:
            raise ValueError('%s is not a valid tokenization scheme/analyzer' %
                             self.analyzer)

    def _validate_vocabulary(self):
        vocabulary = self.vocabulary
        # vocabulary 不为空
        if vocabulary is not None:
            if isinstance(vocabulary, set):
                vocabulary = sorted(vocabulary)
            if not isinstance(vocabulary, Mapping):
                vocab = {}
                for i, t in enumerate(vocabulary):
                    if vocab.setdefault(t, i) != i:
                        msg = "Duplicate term in vocabulary: %r" % t
                        raise ValueError(msg)
                vocabulary = vocab
            else:
                indices = set(vocabulary.itervalues())
                if len(indices) != len(vocabulary):
                    raise ValueError("Vocabulary contains repeated indices.")
                for i in xrange(len(vocabulary)):
                    if i not in indices:
                        msg = ("Vocabulary of size %d doesn't contain index "
                               "%d." % (len(vocabulary), i))
                        raise ValueError(msg)
            if not vocabulary:
                raise ValueError("empty vocabulary passed to fit")
            self.fixed_vocabulary_ = True
            self.vocabulary_ = dict(vocabulary)
        else:
            self.fixed_vocabulary_ = False


class CountVectorizer(VectorizerMixin):
    """
    将文档集转为一个 token 计数矩阵，矩阵采用 scipy.sparse.coo_matrix 稀疏矩阵。.

    If you do not provide an a-priori dictionary and you do not use an analyzer
    that does some kind of feature selection then the number of features will
    be equal to the vocabulary size found by analyzing the data.


    Parameters
    ----------
    analyzer : string, {'word', 'char'} or callable
        Whether the feature should be made of word or character n-grams.
        Option 'char_wb' creates character n-grams only from text inside
        word boundaries.

        If a callable is passed it is used to extract the sequence of features
        out of the raw, unprocessed input.

    preprocessor : callable or None (default)
        Override the preprocessing (string transformation) stage while
        preserving the tokenizing and n-grams generation steps.

    tokenizer : callable or None (default)
        Override the string tokenization step while preserving the
        preprocessing and n-grams generation steps.
        Only applies if ``analyzer == 'word'``.

    stop_words : string {'english'}, list, or None (default)
        If 'english', a built-in stop word list for English is used.

        If a list, that list is assumed to contain stop words, all of which
        will be removed from the resulting tokens.
        Only applies if ``analyzer == 'word'``.

        If None, no stop words will be used. max_df can be set to a value
        in the range [0.7, 1.0) to automatically detect and filter stop
        words based on intra corpus document frequency of terms.

    lowercase : boolean, True by default
        Convert all characters to lowercase before tokenizing.

    token_pattern : string
        Regular expression denoting what constitutes a "token", only used
        if ``analyzer == 'word'``. The default regexp select tokens of 2
        or more alphanumeric characters (punctuation is completely ignored
        and always treated as a token separator).

    vocabulary : Mapping or iterable, optional
        Either a Mapping (e.g., a dict) where keys are terms and values are
        indices in the feature matrix, or an iterable over terms. If not
        given, a vocabulary is determined from the input documents. Indices
        in the mapping should not be repeated and should not have any gap
        between 0 and the largest index.

    binary : boolean, default=False
        If True, all non zero counts are set to 1. This is useful for discrete
        probabilistic models that model binary events rather than integer
        counts.

    dtype : type, optional
        Type of the matrix returned by fit_transform() or transform().

    doc_type : str, {'number', 'str'}, default='str'

    is_record_analyzer_res : boolean, default=False

    Attributes
    ----------
    vocabulary_ : dict
        A mapping of terms to feature indices.

    stop_words_ : set
        Terms that were ignored because they either:

          - occurred in too many documents (`max_df`)
          - occurred in too few documents (`min_df`)
          - were cut off by feature selection (`max_features`).

        This is only available if no vocabulary was given.

    Notes
    -----
    The ``stop_words_`` attribute can get large and increase the model size
    when pickling. This attribute is provided only for introspection and can
    be safely removed using delattr or set to None before pickling.
    """

    def __init__(self, doc_type='str', vocabulary=None, binary=False,
                 lowercase=True, preprocessor=None, tokenizer=None,
                 stop_words=None, token_pattern=r"(?u)\b\w\w+\b",
                 analyzer='word', is_record_analyzer_res=False,
                 stop_number=-1, dtype=np.int8):

        self.preprocessor = preprocessor
        self.tokenizer = tokenizer
        self.analyzer = analyzer
        self.lowercase = lowercase
        self.token_pattern = token_pattern
        self.stop_words = stop_words
        self.vocabulary = vocabulary
        self.binary = binary
        self.stop_number = stop_number
        self.dtype = dtype
        self.doc_type = doc_type
        self.is_record_analyzer_res = is_record_analyzer_res
        self.documents_analyzer_res = []

    def _sort_features(self, X, vocabulary):
        """Sort features by name

        Returns a reordered matrix and modifies the vocabulary in place
        """
        sorted_features = sorted(vocabulary.iteritems())
        map_index = np.empty(len(sorted_features), dtype=np.int32)
        for new_val, (term, old_val) in enumerate(sorted_features):
            map_index[new_val] = old_val
            vocabulary[term] = new_val
        return X[:, map_index]

    def _count_vocab(self, raw_documents, fixed_vocab):
        """Create sparse feature matrix, and vocabulary where fixed_vocab=False
        """
        if fixed_vocab:
            vocabulary = self.vocabulary_
        else:
            # Add a new value when a new vocabulary item is seen
            vocabulary = defaultdict()
            vocabulary.default_factory = vocabulary.__len__

        j_indices = []
        indptr = _make_int_array()
        values = _make_int_array()
        indptr.append(0)

        if self.doc_type == "number":
            for doc in raw_documents:
                if doc == self.stop_number:
                    indptr.append(len(j_indices))
                    continue
                feature_counter = {}
                try:
                    feature_idx = vocabulary[doc]
                    if feature_idx not in feature_counter:
                        feature_counter[feature_idx] = 1
                    else:
                        feature_counter[feature_idx] += 1
                except KeyError:
                    # 使用自定义词库表时，如果文档中包含的词汇[特征]不在自定义词库表中，
                    # 则不再将该词汇[特征]加入到词库表
                    # Ignore out-of-vocabulary items for fixed_vocab=True
                    continue

                j_indices.extend(feature_counter.keys())
                values.extend(feature_counter.values())
                indptr.append(len(j_indices))
        else:
            # 生成文档的分词方法。
            analyze = self.build_analyzer()

            for doc in raw_documents:
                feature_counter = {}
                analyzer_res = analyze(doc)

                if self.is_record_analyzer_res:
                    self.documents_analyzer_res.append(analyzer_res)
                for feature in analyzer_res:
                    try:
                        feature_idx = vocabulary[feature]
                        if feature_idx not in feature_counter:
                            feature_counter[feature_idx] = 1
                        else:
                            feature_counter[feature_idx] += 1
                    except KeyError:
                        # 使用自定义词库表时，如果文档中包含的词汇[特征]不在自定义词库表中，
                        # 则不再将该词汇[特征]加入到词库表
                        # Ignore out-of-vocabulary items for fixed_vocab=True
                        continue

                j_indices.extend(feature_counter.keys())
                values.extend(feature_counter.values())
                indptr.append(len(j_indices))

        # 自动生成[学习]词库表的情况
        if not fixed_vocab:
            # disable defaultdict behaviour
            vocabulary = dict(vocabulary)
            if not vocabulary:
                raise ValueError("empty vocabulary; perhaps the documents only"
                                 " contain stop words")

        j_indices = np.asarray(j_indices, dtype=np.intc)
        indptr = np.frombuffer(indptr, dtype=np.intc)
        values = frombuffer_empty(values, dtype=np.intc)

        X = sp.csr_matrix((values, j_indices, indptr),
                          shape=(len(indptr) - 1, len(vocabulary)),
                          dtype=self.dtype)
        #
        X.sort_indices()
        return vocabulary, X

    def fit_transform(self, raw_documents):
        """Learn the vocabulary dictionary and return term-document matrix.

        Parameters
        ----------
        raw_documents : iterable
            An iterable which yields either str, unicode or file objects.

        Returns
        -------
        X : array, [n_samples, n_features]
            Document-term matrix.
        """
        if isinstance(raw_documents, str):
            raise ValueError(
                "Iterable over raw text documents expected, "
                "string object received.")

        # 校验vocabulary，如果之前提供了自定义vocabulary，则将self.fixed_vocabulary_设为True，否则False
        self._validate_vocabulary()

        vocabulary, X = self._count_vocab(raw_documents,
                                          self.fixed_vocabulary_)

        if self.binary:
            X.data.fill(1)

        # 自动生成[学习]词库表的情况
        if not self.fixed_vocabulary_:
            X = self._sort_features(X, vocabulary)
            self.vocabulary_ = vocabulary
        return X

    def inverse_transform(self, X):
        """Return terms per document with nonzero entries in X.

        Parameters
        ----------
        X : {array, sparse matrix}, shape = [n_samples, n_features]

        Returns
        -------
        X_inv : list of arrays, len = n_samples
            List of arrays of terms.
        """

        if sp.issparse(X):
            # We need CSR format for fast row manipulations.
            X = X.tocsr()
        else:
            # We need to convert X to a matrix, so that the indexing
            # returns 2D objects
            X = np.asmatrix(X)
        n_samples = X.shape[0]

        terms = np.array(list(self.vocabulary_.keys()))
        indices = np.array(list(self.vocabulary_.values()))
        inverse_vocabulary = terms[np.argsort(indices)]

        return [inverse_vocabulary[X[i, :].nonzero()[1]].ravel()
                for i in range(n_samples)]

    def get_feature_names(self):
        """Array mapping from feature integer indices to feature name"""

        # return self.vocabulary_.keys()
        return [k for k, v in sorted(self.vocabulary_.iteritems(), key=lambda item: item[1])]


class MyCountVectorizer(object):
    """
    将文档集转为一个 token 计数矩阵，矩阵采用 scipy.sparse.coo_matrix 稀疏矩阵。.
    文档集中每个文档都只包含一个词或者一个数字。

    Parameters
    ----------

    stop_words : string {'english'}, list, or None (default)
        If 'english', a built-in stop word list for English is used.

        If a list, that list is assumed to contain stop words, all of which
        will be removed from the resulting tokens.
        Only applies if ``analyzer == 'word'``.

        If None, no stop words will be used. max_df can be set to a value
        in the range [0.7, 1.0) to automatically detect and filter stop
        words based on intra corpus document frequency of terms.

    vocabulary : Mapping or iterable, optional
        Either a Mapping (e.g., a dict) where keys are terms and values are
        indices in the feature matrix, or an iterable over terms. If not
        given, a vocabulary is determined from the input documents. Indices
        in the mapping should not be repeated and should not have any gap
        between 0 and the largest index.

    binary : boolean, default=False
        If True, all non zero counts are set to 1. This is useful for discrete
        probabilistic models that model binary events rather than integer
        counts.

    dtype : type, optional
        Type of the matrix returned by fit_transform() or transform().

    Attributes
    ----------
    vocabulary_ : dict
        A mapping of terms to feature indices.

    stop_words_ : set
        Terms that were ignored because they either:

          - occurred in too many documents (`max_df`)
          - occurred in too few documents (`min_df`)
          - were cut off by feature selection (`max_features`).

        This is only available if no vocabulary was given.

    Notes
    -----
    The ``stop_words_`` attribute can get large and increase the model size
    when pickling. This attribute is provided only for introspection and can
    be safely removed using delattr or set to None before pickling.
    """

    def __init__(self, stop_words=None,
                 vocabulary=None, binary=False,
                 dtype=np.int8):
        self.stop_words = stop_words
        self.vocabulary = vocabulary
        self.binary = binary
        self.dtype = dtype

    def _validate_vocabulary(self):
        vocabulary = self.vocabulary
        # vocabulary 不为空
        if vocabulary is not None:
            if isinstance(vocabulary, set):
                vocabulary = sorted(vocabulary)
            if not isinstance(vocabulary, Mapping):
                vocab = {}
                for i, t in enumerate(vocabulary):
                    if vocab.setdefault(t, i) != i:
                        msg = "Duplicate term in vocabulary: %r" % t
                        raise ValueError(msg)
                vocabulary = vocab
            else:
                indices = set(vocabulary.itervalues())
                if len(indices) != len(vocabulary):
                    raise ValueError("Vocabulary contains repeated indices.")
                for i in xrange(len(vocabulary)):
                    if i not in indices:
                        msg = ("Vocabulary of size %d doesn't contain index "
                               "%d." % (len(vocabulary), i))
                        raise ValueError(msg)
            if not vocabulary:
                raise ValueError("empty vocabulary passed to fit")
            self.fixed_vocabulary_ = True
            self.vocabulary_ = dict(vocabulary)
        else:
            self.fixed_vocabulary_ = False

    def _sort_features(self, X, vocabulary):
        """Sort features by name

        Returns a reordered matrix and modifies the vocabulary in place
        """
        sorted_features = sorted(vocabulary.iteritems())
        map_index = np.empty(len(sorted_features), dtype=np.int32)
        for new_val, (term, old_val) in enumerate(sorted_features):
            map_index[new_val] = old_val
            vocabulary[term] = new_val
        return X[:, map_index]

    def _count_vocab(self, raw_documents, fixed_vocab):
        """Create sparse feature matrix, and vocabulary where fixed_vocab=False
        """
        if fixed_vocab:
            vocabulary = self.vocabulary_
        else:
            # Add a new value when a new vocabulary item is seen
            vocabulary = defaultdict()
            vocabulary.default_factory = vocabulary.__len__

        j_indices = []
        indptr = _make_int_array()
        values = _make_int_array()
        indptr.append(0)

        for doc in raw_documents:
            if self.stop_words:
                if doc in self.stop_words:
                    indptr.append(len(j_indices))
                    continue
            feature_counter = {}
            try:
                feature_idx = vocabulary[doc]
                if feature_idx not in feature_counter:
                    feature_counter[feature_idx] = 1
                else:
                    feature_counter[feature_idx] += 1
            except KeyError:
                # 使用自定义词库表时，如果文档中包含的词汇[特征]不在自定义词库表中，
                # 则不再将该词汇[特征]加入到词库表
                # Ignore out-of-vocabulary items for fixed_vocab=True
                continue

            j_indices.extend(feature_counter.keys())
            values.extend(feature_counter.values())
            indptr.append(len(j_indices))

        # 自动生成[学习]词库表的情况
        if not fixed_vocab:
            # disable defaultdict behaviour
            vocabulary = dict(vocabulary)
            if not vocabulary:
                raise ValueError("empty vocabulary; perhaps the documents only"
                                 " contain stop words")

        j_indices = np.asarray(j_indices, dtype=np.intc)
        indptr = np.frombuffer(indptr, dtype=np.intc)
        values = frombuffer_empty(values, dtype=np.intc)

        X = sp.csr_matrix((values, j_indices, indptr),
                          shape=(len(indptr) - 1, len(vocabulary)),
                          dtype=self.dtype)
        #
        X.sort_indices()
        return vocabulary, X

    def fit_transform(self, raw_documents):
        """Learn the vocabulary dictionary and return term-document matrix.

        Parameters
        ----------
        raw_documents : iterable
            An iterable which yields either str, unicode or file objects.

        Returns
        -------
        X : array, [n_samples, n_features]
            Document-term matrix.
        """
        if isinstance(raw_documents, str):
            raise ValueError(
                "Iterable over raw text documents expected, "
                "string object received.")

        # 校验vocabulary，如果之前提供了自定义vocabulary，则将self.fixed_vocabulary_设为True，否则False
        self._validate_vocabulary()

        vocabulary, X = self._count_vocab(raw_documents,
                                          self.fixed_vocabulary_)

        if self.binary:
            X.data.fill(1)

        # 自动生成[学习]词库表的情况
        if not self.fixed_vocabulary_:
            X = self._sort_features(X, vocabulary)
            self.vocabulary_ = vocabulary
        return X

    def inverse_transform(self, X):
        """Return terms per document with nonzero entries in X.

        Parameters
        ----------
        X : {array, sparse matrix}, shape = [n_samples, n_features]

        Returns
        -------
        X_inv : list of arrays, len = n_samples
            List of arrays of terms.
        """

        if sp.issparse(X):
            # We need CSR format for fast row manipulations.
            X = X.tocsr()
        else:
            # We need to convert X to a matrix, so that the indexing
            # returns 2D objects
            X = np.asmatrix(X)
        n_samples = X.shape[0]

        terms = np.array(list(self.vocabulary_.keys()))
        indices = np.array(list(self.vocabulary_.values()))
        inverse_vocabulary = terms[np.argsort(indices)]

        return [inverse_vocabulary[X[i, :].nonzero()[1]].ravel()
                for i in range(n_samples)]

    def get_feature_names(self):
        """Array mapping from feature integer indices to feature name"""

        # return self.vocabulary_.keys()
        return [k for k, v in sorted(self.vocabulary_.iteritems(), key=lambda item: item[1])]


def _check_stop_list(stop):
    if stop == "english":
        return ENGLISH_STOP_WORDS
    elif isinstance(stop, str):
        raise ValueError("not a built-in stop list: %s" % stop)
    elif stop is None:
        return None
    else:               # assume it's a collection
        return frozenset(stop)


def _document_frequency(X):
    """Count the number of non-zero values for each feature in sparse X."""
    if sp.isspmatrix_csr(X):
        return np.bincount(X.indices, minlength=X.shape[1])
    else:
        return np.diff(sp.csc_matrix(X, copy=False).indptr)


def _make_int_array():
    """Construct an array.array of a type suitable for scipy.sparse indices."""
    return array.array(str("i"))

if __name__ == '__main__':
    import pandas as pd
    import time
    import jieba

    t0 = time.clock()

    # docs = [1, 0, 2, 0]
    docs = [0, 0, 0]
    m = CountVectorizer(doc_type="number", stop_number=0)

    # docs = ["and Abc 123 Bcd", "abc"]
    # m = CountVectorizer(doc_type="str", analyzer="word", stop_words="english", is_record_analyzer_res=True)

    # docs = [u"蓝迪智慧乐园 儿童磁性彩色绘画板", u"蓝色燕窝 洗发水"]
    # m = CountVectorizer(doc_type="str", analyzer="word", stop_words=[" "],
    #                     preprocessor=None, tokenizer=jieba.cut, is_record_analyzer_res=True)

    vectors = m.fit_transform(docs)
    print "docs: %s" % docs
    print "documents_analyzer_res: \n%s" % pd.Series(m.documents_analyzer_res)

    print "feature: %s" % ", ".join(m.get_feature_names()) if isinstance(m.get_feature_names()[0], str) else "feature: %s" % m.get_feature_names()
    print "res vector: \n%s" % vectors.toarray()

