package main

const (
	embedMaxChars     = 2000 // COMMONCRAWL_EMBED_MAX_CHARS default
	marginThreshold   = 0.03 // confident if top1-top2 >= this
	pageTypeThreshold = 0.55 // page-type detected if max prototype sim >= this
	confTopK          = 10   // nace_confidence = softmax over the top-K standardized scores
	confTemp          = 1.0  // softmax temperature over median/MAD-standardized z-scores (tune on labels)
	embedBatch        = 16 // texts/request; small so requests co-batch under the engine token budget
	ccBucket          = "commoncrawl"
	techMaxBytes      = 131072 // cap body fed to Wappalyzer; full-body regex over MB pages ~= 1.2s/page
)
