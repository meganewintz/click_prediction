train <- cleanData[1:500000,]
test <- cleanData[500001:1000000,]

# Make the model
model <- glm(train$label~.,family="binomial", data=train)

# Anova
anova(model, test="Chisq")

# R2
library(pscl)
pR2(model)

# Result of our prediction on test data
fitted.results <- predict(model,type="response")

fitted.results <- predict(model,newdata=test, type="response")

fitted.results <- predict(model,newdata=subset(test,select=c(0:35)), type="response")
fitted.results <- ifelse(fitted.results > 0.5,1,0)

misClasificError <- mean(fitted.results != test$label)
print(paste('Accuracy',1-misClasificError))

table(fitted.results,train$label)

# Confusion matrix
library(caret)
confusionMatrix(data=fitted.results, reference=test$label)

# Display the ROC plat
library(ROCR)
p <- predict(model, newdata=subset(test,select=c(0:34)), type="response")
pr <- prediction(p, test$label)
prf <- performance(pr, measure = "tpr", x.measure = "fpr")
plot(prf)

auc <- performance(pr, measure = "auc")
auc <- auc@y.values[[1]]
auc



mod <- glm(cleanData$label~.,family="binomial", data=cleanData)
summary(mod)

modBackword <- step(mod, direction="backward")
summary(modBackword)

summary(glm(cleanData$label~appOrSite,family="binomial", data=cleanData))

