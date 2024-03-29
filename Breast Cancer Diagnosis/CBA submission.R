library(caTools)
library(data.table)
library(ggplot2)
library(neuralnet)
library(randomForest)
library(car)
library(caret)
library(e1071)

data1 <- fread("wisconsin breast cancer data.csv", stringsAsFactors = T)
summary(data1)
data1[duplicated(data1)] # check for duplication
sum(is.na(data1)) # check for any NA, although can be seen in summary(data1)
# rename concave points to concave_points for easier coding
setnames(data1, "concave points_mean", "concave_points_mean")
setnames(data1, "concave points_se", "concave_points_se")
setnames(data1, "concave points_worst", "concave_points_worst")
# change Malignant to 1, Benign to 0.
data1[,diagnosis:=ifelse(diagnosis=="M",1,0)]
data1$diagnosis<-factor(data1$diagnosis)
# remove id which is not used
data1[,id:=NULL]
summary(data1)
# Since standard error of the measurement is not the measurement of the tumour, remove standard error of all measurements. 
data1[,":=" (radius_se=NULL, texture_se=NULL, perimeter_se=NULL, area_se=NULL, smoothness_se=NULL, compactness_se=NULL, concavity_se=NULL, concave_points_se=NULL, symmetry_se=NULL, fractal_dimension_se=NULL)]

table1=data.frame(matrix(NA,ncol(data1),ncol(data1)))
test <-as.data.frame.matrix(data1)
summary(test)
for (i in c(2:20)){
  table1[1,i]<-colnames(data1)[i]
  table1[i,1]<-colnames(data1)[i]
  for (j in c(2:20)){
    if (i != j){
      table1[i,j]<-cor(test[,i],test[,j])
    }
  }
}
# correlation table 1
table1
# Worst seems to be highly correlated with the mean since average of the top 3 measurements is just larger than or equals to the average of all measurements for a tumour. Thus, should remove all worst.
# However, Symmetry_mean and worst is not as highly correlated (0.6998), while fractal_dimension_mean and worst is also not (0.7673), thus, will remove everything else other than these two worst.

data1[,":=" (radius_worst=NULL, texture_worst=NULL, perimeter_worst=NULL, area_worst=NULL, smoothness_worst=NULL, compactness_worst=NULL, concavity_worst=NULL, concave_points_worst=NULL)]

table2=data.frame(matrix(NA,ncol(data1),ncol(data1)))
test <-as.data.frame.matrix(data1)
summary(test)
for (i in c(2:ncol(data1))){
  table2[1,i]<-colnames(data1)[i]
  table2[i,1]<-colnames(data1)[i]
  for (j in c(2:ncol(data1))){
    if (i != j){
      table2[i,j]<-cor(test[,i],test[,j])
    }
  }
}
# second cor table
table2
# will remove radius, perimeter, and concave points, leaving area.
# will remove compactness, leaving concavity.

data1[,":=" (radius_mean=NULL, perimeter_mean=NULL, compactness_mean=NULL, concave_points_mean=NULL)]

table3=data.frame(matrix(NA,ncol(data1),ncol(data1)))
test <-as.data.frame.matrix(data1)
summary(test)
for (i in c(2:ncol(data1))){
  table3[1,i]<-colnames(data1)[i]
  table3[i,1]<-colnames(data1)[i]
  for (j in c(2:ncol(data1))){
    if (i != j){
      table3[i,j]<-cor(test[,i],test[,j])
    }
  }
}
# 3rd cor table for final check
table3
summary(data1)

# calculating VIF
test.mod<-glm(diagnosis~.,family=binomial, data=data1)
vif(test.mod)
# Since our sample size is rather small, we will use a stricter VIF threshold of 5, and will remove fractal_dimension_mean
data1[, fractal_dimension_mean:=NULL]
test.mod2<-glm(diagnosis~.,family=binomial, data=data1)
vif(test.mod2)

test<-data1
# Exploratory data analysis
ggplot(test)+aes(y=texture_mean)+geom_boxplot() 
ggplot(test)+aes(y=area_mean)+geom_boxplot()
ggplot(test)+aes(y=smoothness_mean)+geom_boxplot() 
ggplot(test)+aes(y=concavity_mean)+geom_boxplot()
ggplot(test)+aes(y=symmetry_mean)+geom_boxplot()
ggplot(test)+aes(y=symmetry_worst)+geom_boxplot()
ggplot(test)+aes(y=fractal_dimension_worst)+geom_boxplot()
# seem to have many positively-skewed data (right-tailed), thus, test for skewness
#skewness
skewness(data1$texture_mean)
skewness(data1$area_mean)
skewness(data1$smoothness_mean)
skewness(data1$concavity_mean)
skewness(data1$symmetry_mean)
skewness(data1$symmetry_worst)
skewness(data1$fractal_dimension_worst)
# since most data is right-tailed, will do log transformation for skewness>1 (area_mean, concavity_mean, symmetry_worst, fractal_dimension_worst), but since the concavity_mean, symmetry_mean and fractal_dimension_worst is below 1 and concavity_mean has 0 value within data, will use log(x+1) instead of just log(x).
data1$area_mean<-log(data1$area_mean+1)
data1$concavity_mean<-log(data1$concavity_mean+1)
data1$symmetry_worst<-log(data1$symmetry_worst+1)
data1$fractal_dimension_worst<-log(data1$fractal_dimension_worst+1)

# normalize data with min=0, max=1 (feature scaling)
process<-preProcess(data1,method=c("range"))
norm_scale<-predict(process,data1)
data1<-norm_scale

data1
ggplot(data1)+aes(y=texture_mean)+geom_boxplot() 
ggplot(data1)+aes(y=area_mean)+geom_boxplot()
ggplot(data1)+aes(y=smoothness_mean)+geom_boxplot() 
ggplot(data1)+aes(y=concavity_mean)+geom_boxplot()
ggplot(data1)+aes(y=symmetry_mean)+geom_boxplot()
ggplot(data1)+aes(y=symmetry_worst)+geom_boxplot()
ggplot(data1)+aes(y=fractal_dimension_worst)+geom_boxplot()

test<-data1

#remove outliers
outlier.value<-1.5*(summary(test$texture_mean)[5]-summary(test$texture_mean)[2])
data1 <-data1[texture_mean<=outlier.value+summary(test$texture_mean)[5] & texture_mean >=summary(test$texture_mean)[2]-outlier.value | is.na(texture_mean)]

outlier.value<-1.5*(summary(test$area_mean)[5]-summary(test$area_mean)[2])
data1 <-data1[area_mean<=outlier.value+summary(test$area_mean)[5] & area_mean >=summary(test$area_mean)[2]-outlier.value | is.na(area_mean)]

outlier.value<-1.5*(summary(test$smoothness_mean)[5]-summary(test$smoothness_mean)[2])
data1 <-data1[smoothness_mean<=outlier.value+summary(test$smoothness_mean)[5] & smoothness_mean >=summary(test$smoothness_mean)[2]-outlier.value | is.na(smoothness_mean)]

outlier.value<-1.5*(summary(test$concavity_mean)[5]-summary(test$concavity_mean)[2])
data1 <-data1[concavity_mean<=outlier.value+summary(test$concavity_mean)[5] & concavity_mean >=summary(test$concavity_mean)[2]-outlier.value | is.na(concavity_mean)]

outlier.value<-1.5*(summary(test$symmetry_mean)[5]-summary(test$symmetry_mean)[2])
data1 <-data1[symmetry_mean<=outlier.value+summary(test$symmetry_mean)[5] & symmetry_mean >=summary(test$symmetry_mean)[2]-outlier.value | is.na(symmetry_mean)]

outlier.value<-1.5*(summary(test$symmetry_worst)[5]-summary(test$symmetry_worst)[2])
data1 <-data1[symmetry_worst<=outlier.value+summary(test$symmetry_worst)[5] & symmetry_worst >=summary(test$symmetry_worst)[2]-outlier.value | is.na(symmetry_worst)]

outlier.value<-1.5*(summary(test$fractal_dimension_worst)[5]-summary(test$fractal_dimension_worst)[2])
data1 <-data1[fractal_dimension_worst<=outlier.value+summary(test$fractal_dimension_worst)[5] & fractal_dimension_worst >=summary(test$fractal_dimension_worst)[2]-outlier.value | is.na(fractal_dimension_worst)]

summary(data1)
sum(data1$diagnosis==1)/nrow(data1)
# 33% Malignant, 67% Benign, not very imbalanced, so no down/up sampling required
# 164/504

# train test split
set.seed(234)
train1<-sample.split(Y=data1$diagnosis,SplitRatio = 0.7)
trainset<-subset(data1,train1==T)
testset<-subset(data1,train1==F)
trainset
testset

# Random Forest
ncol(data1) # 8 but -1 for diagnosis so 7
floor(log(7,2)+1) # 3 for mtry
# calibrating for optimal number of trees with OOB error
B=c(100:700)
OOB.error<-seq(1:length(B))
for (i in 1:length(B)){
  set.seed(234)
  m.RF<- randomForest(diagnosis ~ . , data = trainset,mtry=3,ntree=B[i])
  OOB.error[i]<-m.RF$err.rate[m.RF$ntree,1]
}
OOB.error
results<- data.frame(B, OOB.error)
results 
ggplot(results)+aes(y=OOB.error, x=B)+geom_line()+geom_point()
# lowest OOB error appears at 367 and 370, and second lowest appears and stabilizing between 321 and 394. Thus, will use 367, closer to midpoint of 357.5
set.seed(234)
m.RF.1 <- randomForest(diagnosis ~ . , data = trainset, mtry=3,ntree=367, importance = T)
m.RF.1
# trainset model and confusion matrix
rf.acc=mean(testset$diagnosis==predict(m.RF.1, newdata = testset)) # Accuracy
rf.cm<-table("testset"=testset$diagnosis,"Random Forest"=predict(m.RF.1, newdata = testset)) # testset confusion matrix
rf.fp<-rf.cm[1,2]/(rf.cm[1,2]+rf.cm[1,1]) # False Positive
rf.fn<-rf.cm[2,1]/(rf.cm[2,1]+rf.cm[2,2]) # False Negative
rf.precision<-rf.cm[2,2]/(rf.cm[1,2]+rf.cm[2,2]) # Precision
rf.recall<-rf.cm[2,2]/(rf.cm[2,1]+rf.cm[2,2]) # Recall

# 10-fold cv logistic regression
cv<- trainControl(method = "cv", number=10)
set.seed(234)
glm1 <- train(diagnosis~., data=trainset, method="glm", trControl=cv)
summary(glm1)
# symmetry_mean and fractal_dimension_worst more than p-value of 0.05. Thus, not included in the next model
set.seed(234)
glm2<-train(diagnosis~.-symmetry_mean-fractal_dimension_worst, data=trainset, method="glm", trControl=cv)
summary(glm2)
table("trainset"=trainset$diagnosis,"Logistic Regression"=predict(glm2))
# trainset confusion matrix
glm.acc<-mean(testset$diagnosis==predict(glm2, newdata = testset)) # accuracy
glm.cm<-table("testset"=testset$diagnosis, "Logistic Regression"=predict(glm2, newdata = testset))# testset confusion matrix
glm.fp<-glm.cm[1,2]/(glm.cm[1,2]+glm.cm[1,1]) # False Positive
glm.fn<-glm.cm[2,1]/(glm.cm[2,1]+glm.cm[2,2]) # False Negative
glm.precision<-glm.cm[2,2]/(glm.cm[1,2]+glm.cm[2,2]) # Precision
glm.recall<-glm.cm[2,2]/(glm.cm[2,1]+glm.cm[2,2]) # Recall

# Neural Network
hidden.list <- list(2,3,4,5,c(2,2),c(2,3),c(2,4),c(2,5),c(3,2), c(3,3),c(3,4),c(3,5),c(4,2),c(4,3),c(4,4),c(4,5),c(5,2),c(5,3),c(5,4),c(5,5)) # calibrate for 1 or 2 hidden layer for 2,3,4, or 5 nodes in each hidden layer
nn.test<-data.frame(matrix(NA,length(hidden.list),6))
colnames(nn.test) = c("number","Accuracy","False Positive", "False Negative", "Precision", "Recall")
for (i in 1:length(hidden.list)){
  nn.test[i,1]<-i
  set.seed(234)
  m1<-NA
  m1 <- neuralnet(diagnosis~., data=trainset, hidden=unlist(hidden.list[i]), err.fct="ce", linear.output=FALSE, algorithm="rprop+")
  if (is.null(m1$net.result)==FALSE){# in cases where algorithm does not converge
      out <- as.data.frame(m1$net.result)
      pred.m1 <- ifelse(unlist(out[,2]) > 0.5, 1, 0)
      table(trainset$diagnosis, pred.m1)
      test.pred <- ifelse(predict(m1,newdata=testset)[,2]>0.5,1,0)
      nn.cm<-table(testset$diagnosis,test.pred)
      nn.test[i,2]<-mean(test.pred == testset$diagnosis)
      nn.test[i,3]<-nn.cm[1,2]/(nn.cm[1,2]+nn.cm[1,1]) # False Positive
      nn.test[i,4]<-nn.cm[2,1]/(nn.cm[2,1]+nn.cm[2,2]) # False Negative
      nn.test[i,5]<-nn.cm[2,2]/(nn.cm[1,2]+nn.cm[2,2]) # Precision
      nn.test[i,6]<-nn.cm[2,2]/(nn.cm[2,1]+nn.cm[2,2]) # Recall
    }
}

nn.test 
# model 10 (c(3,3)) and 20(c(5,5)) have the same and the best values for all 5 scores. Highest accuracy, precision and recall while lowest false negative and false positive.
set.seed(234)
# model 10
nn1 <- neuralnet(diagnosis~., data=trainset, hidden=c(3,3), err.fct="ce", linear.output=FALSE, algorithm="rprop+")
nn1$weights
plot(nn1)
set.seed(234)
# model 20
nn2 <- neuralnet(diagnosis~., data=trainset, hidden=c(5,5), err.fct="ce", linear.output=FALSE, algorithm="rprop+")
nn2$weights
plot(nn2)
out.nn1<-as.data.frame(nn1$net.result) # output of model 10
pred.nn1 <- ifelse(unlist(out.nn1[,2]) > 0.5, 1, 0) 
pre.nn1<-ifelse(unlist(out.nn1[,1]) > 0.5, 0, 1)
mean(pre.nn1==pred.nn1) # using either output is the same
out.nn2<-as.data.frame(nn2$net.result) # output of model 10
pred.nn2 <- ifelse(unlist(out.nn2[,2]) > 0.5, 1, 0)
table("trainset"=trainset$diagnosis,"neuralnet 1"=pred.nn1)
table("trainset"=trainset$diagnosis,"neuralnet 2"=pred.nn2) 
# model 20 has no error in classifying trainset with nn2
test.pred.nn1 <- ifelse(predict(nn1,newdata=testset)[,2]>0.5,1,0)
test.pred.nn2 <- ifelse(predict(nn2,newdata=testset)[,2]>0.5,1,0)
mean(test.pred.nn2==test.pred.nn1) 
# test set predictions different 
table("testset"=testset$diagnosis,"neuralnet 1"=test.pred.nn1)
table("testset"=testset$diagnosis,"neuralnet 2"=test.pred.nn2) 
# no difference in test set results (confusion matrix), so just use 1st one
nn.cm<-table("testset"=testset$diagnosis,"neuralnet"=test.pred.nn1)
nn.acc<- mean(test.pred.nn1 == testset$diagnosis)
nn.fp<-nn.cm[1,2]/(nn.cm[1,2]+nn.cm[1,1]) # False Positive
nn.fn<-nn.cm[2,1]/(nn.cm[2,1]+nn.cm[2,2]) # False Negative
nn.precision<-nn.cm[2,2]/(nn.cm[1,2]+nn.cm[2,2]) # Precision
nn.recall<-nn.cm[2,2]/(nn.cm[2,1]+nn.cm[2,2]) # Recall

final.results<-data.table(Model=c("Random Forest", "Logistic Regression", "Multi Layer Perceptron"), Accuracy=c(rf.acc, glm.acc, nn.acc), False_Positive=c(rf.fp,glm.fp,nn.fp),False_Negative=c(rf.fn,glm.fn,nn.fn), Precision=c(rf.precision,glm.precision,nn.precision),Recall=c(rf.recall,glm.recall,nn.recall))

# importance of variables
var.impt <- importance(m.RF.1)
var.impt
# alternative
varImpPlot(m.RF.1, type = 1)
varImpPlot(m.RF.1, type = 2)
# Random forest: area_mean, concavity_mean, texture_mean, smoothness_mean/symmetry worst, fractal_dimension_worst, symmetry_mean
summary(glm2)
# Logistic regression: area_mean, texture_mean, smoothness_mean, concavity_mean, symmetry_worst

# time taken to predict/diagnose
# RF
a=0
time.taken<-function(model.used, test=testset){
  for (i in 1:10){
    start<-Sys.time()
    predict(model.used, newdata = test)
    stop<-Sys.time()
    a=a+stop-start
  }
  return(a)
}
rf.time<-time.taken(m.RF.1)
glm.time<-time.taken(glm2)
nn.time<-time.taken(nn1)
# add results to final table
final.results[,Time_Taken:=c(rf.time, glm.time,nn.time)]

library(xgboost)
library(Matrix)

sparse_matrix <- sparse.model.matrix(diagnosis ~ ., data = trainset)[,-1]

output_vector <- trainset[,diagnosis] == 1

bst <- xgboost(data = sparse_matrix, label = output_vector, max.depth = 200, eta = 1, nrounds = 5,nthread = 10, objective = "binary:logistic")

sparse_matrix_test <- sparse.model.matrix(diagnosis ~ ., data = testset)[,-1]
test.pred.xgb=as.numeric(predict(bst, newdata = sparse_matrix_test) > 0.5)
test.pred.xgb <- as.numeric(test.pred.xgb > 0.5)

xgb.cm<-table("testset"=testset$diagnosis,"XGBoost"=test.pred.xgb)
xgb.acc<- mean(test.pred.nn1 == testset$diagnosis)
xgb.fp<-xgb.cm[1,2]/(xgb.cm[1,2]+xgb.cm[1,1]) # False Positive
xgb.fn<-xgb.cm[2,1]/(xgb.cm[2,1]+xgb.cm[2,2]) # False Negative
xgb.precision<-xgb.cm[2,2]/(xgb.cm[1,2]+xgb.cm[2,2]) # Precision
xgb.recall<-xgb.cm[2,2]/(xgb.cm[2,1]+xgb.cm[2,2]) # Recall
xgb.time<-time.taken(bst,sparse_matrix_test)

final.results.2<-data.table(Model=c("Random Forest", "Logistic Regression", "Multi Layer Perceptron", "XGBoost"), Accuracy=c(rf.acc, glm.acc, nn.acc, xgb.acc), False_Positive=c(rf.fp,glm.fp,nn.fp, xgb.fp),False_Negative=c(rf.fn,glm.fn,nn.fn, xgb.fn), Precision=c(rf.precision,glm.precision,nn.precision, xgb.precision),Recall=c(rf.recall,glm.recall,nn.recall, xgb.recall))

final.results.2[,Time_Taken:=c(rf.time, glm.time,nn.time,xgb.time)]
