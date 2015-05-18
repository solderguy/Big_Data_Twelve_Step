step7 <- function() {
    d<-read.table("../output_files/out_s7.tab", header = TRUE,
         sep="\t", colClasses=c("numeric","numeric","numeric"))
    pdf(file="../image_files/step7.pdf")
    plot(d$x,d$y, xlim=c(0,119), ylim=c(0,119))
    title(main="Step 7: Original points")
    dev.off()
}

step8 <- function() {
    d<-read.csv("../output_files/s8out_h.csv", as.is=TRUE)
    pdf(file="../image_files/step8.pdf")
    plot(d$x,d$y, xlim=c(0,119), ylim=c(0,119))
    title(main="Step 8: After outliers removed")
    dev.off()
}

step9 <- function() {
    d<-read.csv("../output_files/s9out_h.csv", as.is=TRUE)
    pdf(file="../image_files/step9.pdf")
    plot(d$x,d$y, xlim=c(0,119), ylim=c(0,119))
    title(main="Step 9: After noise cleaning")
    dev.off()
}
step10 <- function() {
    d<-read.csv("../output_files/s10out.csv", as.is=TRUE)
    pdf(file="../image_files/step10.pdf")
    plot(d$x,d$y, col=d$cluster+2, xlim=c(0,119), ylim=c(0,119))
    title(main="Step 10: Clusters denoted by color")
    dev.off()
}

myArg = commandArgs(TRUE)[1]

if (myArg == "step7") { 
	step7() 
} else if (myArg == "step8") { 
	step8() 
} else if (myArg == "step9") { 
	step9() 
} else if (myArg == "step10") { 
	step10() 
} else { 
	writeLines("bad argument")
}
