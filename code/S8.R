# CREDENTIALS
#   Module: S8.R
#   Author: John Soper
#   Date: Apr 2015
#   Rev: 1
#
# SUMMARY
#     This is the eigth component of the Big Data Twelve Step Program
#     It runs R code to remove the border and outlier points.  After this
#     step, the only undesired points left of the noise ones interspersed w
#     with the valid data 


d<-read.table("../output_files/out_s7.tab", header = TRUE,
     sep="\t", colClasses=c("numeric","numeric","numeric"))

# remove borders
d <- subset(d, d$x > 10 & d$x < 110)
d <- subset(d, d$y > 10 & d$y < 110)

lower_x_q <- quantile(d$x)[2]
upper_x_q <- quantile(d$x)[4]
iqr_x <- upper_x_q - lower_x_q
thr_x_hi <- upper_x_q + 0.8 * iqr_x
thr_x_lo <- lower_x_q - 0.4 * iqr_x

d <- subset(d, d$x > thr_x_lo & d$x < thr_x_hi)
#plot(d$x,d$y, xlim=c(0,119), ylim=c(0,119))

lower_y_q <- quantile(d$y)[2]
upper_y_q <- quantile(d$y)[4]
iqr_y <- upper_y_q - lower_y_q
thr_y_hi <- upper_y_q + 0.8 * iqr_y
thr_y_lo <- lower_y_q - 0.5 * iqr_y

d <- subset(d, d$y > thr_y_lo & d$y < thr_y_hi)

#reorder columns for Step 9 with hadoop processing
d<-d[,c(2,3,1)]

write.table(d, file = "../output_files/s8_out.csv", sep = ",", qmethod = "double", row.names=FALSE, col.names=FALSE)
