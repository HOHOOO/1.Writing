# coding=utf-8

# 分词停用词
# SIGNAL_STOP_WORDS = frozenset([",", "?", "、", "。", "“", "”", "《", "》", "！",
#                                "，", "：", "；", "？", "~", "!", ".", ":", "#",
#                                "@", "`", ";", "$", "（", "）", "——", "—", "￥",
#                                "·", "...", "‘", "’", "〉", "〈", "…", "＞", "＜",
#                                "＠", "＃", "＄", "％", "︿", "＆", "＊", "＋",
#                                "～", "｜", "［", "］", "｛", "｝", "【", "】",
#                                "+", "=", "&", "^", "%", "\\", "|", "-", "_",
#                                "--", "..", ">>", "[", "]", "<", ">", "/",
#                                "\"", "'", "(", ")", "*", " "])

SIGNAL_STOP_WORDS = frozenset([u",", u"?", u"、", u"。", u"“", u"”", u"《", u"》", u"！",
                               u"，", u"：", u"；", u"？", u"~", u"!", u".", u":", u"#",
                               u"@", u"`", u";", u"$", u"（", u"）", u"——", u"—", u"￥",
                               u"·", u"...", u"‘", u"’", u"〉", u"〈", u"…", u"＞", u"＜",
                               u"＠", u"＃", u"＄", u"％", u"︿", u"＆", u"＊", u"＋",
                               u"～", u"｜", u"［", u"］", u"｛", u"｝", u"【", u"】",
                               u"+", u"=", u"&", u"^", u"%", u"\\", u"|", u"-", u"_",
                               u"--", u"..", u">>", u"[", u"]", u"<", u">", u"/",
                               u"\"", u"'", u"(", u")", u"*", u" "])

ENGLISH_STOP_WORDS = frozenset([
"a", "about", "above", "across", "after", "afterwards", "again", "against",
"all", "almost", "alone", "along", "already", "also", "although", "always",
"am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
"any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
"around", "as", "at", "back", "be", "became", "because", "become",
"becomes", "becoming", "been", "before", "beforehand", "behind", "being",
"below", "beside", "besides", "between", "beyond", "bill", "both",
"bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
"could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
"down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
"elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
"everything", "everywhere", "except", "few", "fifteen", "fifty", "fill",
"find", "fire", "first", "five", "for", "former", "formerly", "forty",
"found", "four", "from", "front", "full", "further", "get", "give", "go",
"had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
"hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
"how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
"interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
"latterly", "least", "less", "ltd", "made", "many", "may", "me",
"meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
"move", "much", "must", "my", "myself", "name", "namely", "neither",
"never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
"nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
"once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
"ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
"please", "put", "rather", "re", "same", "see", "seem", "seemed",
"seeming", "seems", "serious", "several", "she", "should", "show", "side",
"since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
"something", "sometime", "sometimes", "somewhere", "still", "such",
"system", "take", "ten", "than", "that", "the", "their", "them",
"themselves", "then", "thence", "there", "thereafter", "thereby",
"therefore", "therein", "thereupon", "these", "they", "thick", "thin",
"third", "this", "those", "though", "three", "through", "throughout",
"thru", "thus", "to", "together", "too", "top", "toward", "towards",
"twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
"very", "via", "was", "we", "well", "were", "what", "whatever", "when",
"whence", "whenever", "where", "whereafter", "whereas", "whereby",
"wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
"who", "whoever", "whole", "whom", "whose", "why", "will", "with",
"within", "without", "would", "yet", "you", "your", "yours", "yourself",
"yourselves"])

if __name__ == "__main__":
    import pandas as pd
    import jieba
    import re

    input_file = "../off_line_file/title_fc_weight_part.xlsx"
    data = pd.read_excel(input_file, encoding="utf_8_sig")

    data[u"title"] = data[u"title"].map(lambda s: re.sub("\n", " ", s.lower()) if s else "")
    sen = "\n".join(data[u"title"].values)
    # data[u"title_fc"] = ",".join(jieba.cut_for_search(sen)).split("\n")  # 对标题进行分词
    res = (",".join(jieba.cut(sen)))
    print SIGNAL_STOP_WORDS
    print res.split("\n")
    res = ",".join([v for v in jieba.cut(sen) if v not in SIGNAL_STOP_WORDS])
    data[u"title_fc"] = [v for v in res.split("\n") if v not in SIGNAL_STOP_WORDS]  # 对标题进行分词
    print data[u"title_fc"]

    # docs = "我不在《》"
    # res = list(jieba.cut(docs))
    # # print ", ".join(res)
    # print "111"
    # print ", ".join([v for v in res if v not in SIGNAL_STOP_WORDS])
    # print "222"
    # print ", ".join([v for v in res if v not in (u"《", u"》")])
    # print "333"


