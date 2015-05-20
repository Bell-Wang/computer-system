devtools::install_github("gaborcsardi/spark")
devtools::install_github("amplab-extras/SparkR-pkg",subdir="pkg")
library(spark)
library(SparkR)
install.packages("tm")
install.packages("NLP")
library(NLP)
library(tm)

sc <- sparkR.init("local")
linesRDD <- textFile(sc, "~/Desktop/test/hw4/data.txt")
words<-lapply(linesRDD,function(line){length(unlist(strsplit(line,"")))})
word<-flatMap(linesRDD,function(line){strsplit(line,split=" ")})

list_words<-collect(words)[[2]]
pre_dd<-Corpus(VectorSource(list_words))
pre_dtm <- DocumentTermMatrix(pre_dd, control = list(weighting = weightTfIdf))
inspect(pre_dtm)
dtm_tfxidf<-weightTfIdf(pre_dtm)
inspect(dtm_tfxidf)

dtm_to_sparse <- function(dtm) {
  sparseMatrix(i=dtm$i, j=dtm$j, x=dtm$v, dims=c(dtm$nrow, dtm$ncol), dimnames=dtm$dimnames)
}
sparseMat<-as.matrix(dtm_to_sparse(pre_dtm))
t_sparseMat<-data.frame(sparseMat)
n_sparseMat<-cbind(n_data$section,t_sparseMat)

#document cluster
### k-means (this uses euclidean distance)
m <- as.matrix(dtm_tfxidf)
rownames(m) <- 1:nrow(m)

norm_eucl <- function(m){
  m/apply(m, MARGIN=1, FUN=function(x) sum(x^2)^.5)}
m_norm <- norm_eucl(m)
### cluster into 50 clusters
cl <- kmeans(m_norm, 50)
findFreqTerms(dtm[cl$cluster==1], 50)
inspect(reuters[which(cl$cluster==1)])

result<-apply(pre_dtm, 1, function(x) {
  x2 <- sort(x, TRUE)
  x2[x2 >= x2[100]]
})

test_re<-lapply(result,as.data.frame)
write.csv(result,"~/Desktop/test.txt")
