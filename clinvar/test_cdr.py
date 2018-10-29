def main():

    from snorkel import SnorkelSession
    session = SnorkelSession()

    import os
    from snorkel.parser import XMLMultiDocPreprocessor

    # The following line is for testing only. Feel free to ignore it.
    file_path = 'data/CDR.BioC.small.xml' if 'CI' in os.environ else 'data/CDR.BioC.xml'

    doc_preprocessor = XMLMultiDocPreprocessor(
        path=file_path,
        doc='.//document',
        text='.//passage/text/text()',
        id='.//id/text()'
    )

    from snorkel.parser import CorpusParser
    from utils import TaggerOneTagger

    tagger_one = TaggerOneTagger()
    corpus_parser = CorpusParser(fn=tagger_one.tag)
    corpus_parser.apply(list(doc_preprocessor)[:100])
    # parsed result saved in session

    return doc_preprocessor, corpus_parser, session


if __name__ == "__main__":
    doc_preprocessor, corpus_parser, session = main()