package main

import (
  "fmt"
  "os"
  "strconv"
  "io/ioutil"
  "sync"
  "math/rand"
  "time"
)

func randomString(len int) string {
    bytes := make([]byte, len)
    for i := 0; i < len; i++ {
        bytes[i] = byte(randInt(97, 122))
    }
    return string(bytes)
}

func randInt(min int, max int) int {
    return min + rand.Intn(max-min)
}

func createDirStruc(dirSlice []string) {
  for _, dir := range dirSlice {
    os.MkdirAll(dir, os.ModePerm)
    os.MkdirAll(dir+"dir0", os.ModePerm)
    fmt.Println(dir + " created")
  }
}

func copyFile(srcFile string, dir string, start int, end int) (int64, error) {
  input, err := ioutil.ReadFile(srcFile)
  if err != nil {
    return 0, err
  }
  for i:=start; i<=end; i++ {
    fmt.Println(srcFile)
    dNum := strconv.Itoa(i/10000)
    subDir := "dir" + dNum + "/"
    if (i%10000) == 0{
      os.Mkdir(dir+subDir, os.ModePerm)
      fmt.Println(dir+subDir + " created")
    }
    suffix := ".jpg"
    rand.Seed(time.Now().UTC().UnixNano())
    fileName := strconv.Itoa(i) + "-" + randomString(12) + suffix
    destFile := dir + subDir + fileName
    fmt.Println("destFile:", destFile)
    err = ioutil.WriteFile(destFile, input, 0644)
    if err != nil {
      fmt.Println("Error creating", destFile)
      return 0, err
    }
  }
  return 0, err
}

func main() {
  // dirSlice := []string{"aFactory/1img/", "aFactory/2img/", "aFactory/3img/", "aFactory/4img/", "aFactory/5img/","bFactory/1img/", "bFactory/2img/", "bFactory/3img/", "bFactory/4img/", "bFactory/5img/"}
  var wg sync.WaitGroup

  rootPath0 := "/fs0/"
  rootPath1 := "/fs1/"
  //rootPath2 := "/data2/"
  //rootPath3 := "/data3/"
  //rootPath4 := "/data4/"
  //rootPath5 := "/data5/"
  //rootPath2 := "/home/ec2-user/efs2/"
  //rootPath3 := "/home/ec2-user/efs3/"
  //rootPath4 := "/home/ec2-user/efs4/"
  //rootPath5 := "/home/ec2-user/efs5/"
  //rootPath6 := "/home/ec2-user/efs6/"
  //rootPath7 := "/home/ec2-user/efs7/"
  //rootPath8 := "/home/ec2-user/efs8/"
  //rootPath9 := "/home/ec2-user/efs9/"

  wg.Add(1)
  defer wg.Done()

  srcFile1 := "images/50KB.jpg"
  dirSlice1 := []string{
	  rootPath0+"aFactory/50KB/", 
	  rootPath0+"bFactory/50KB/", 
	  rootPath1+"cFactory/50KB/", 
	  rootPath1+"dFactory/50KB/",
  }
  //dirSlice1 := []string{rootPath0+"aFactory/50B/", rootPath0+"bFactory/50B/", rootPath1+"cFactory/50B/", rootPath1+"dFactory/50B/"}
  createDirStruc(dirSlice1)
  for _, dir := range dirSlice1 {
    go copyFile(srcFile1, dir, 1, 5000000)
  }
  /*
  srcFile2 := "images/2img.jpg"
  dirSlice2 := []string{rootPath0+"aFactory/2img/", rootPath0+"bFactory/2img/", rootPath1+"cFactory/2img/", rootPath1+"dFactory/2img/"}
  createDirStruc(dirSlice2)
  for _, dir := range dirSlice2 {
    go copyFile(srcFile2, dir, 0, 76841)
  }

  srcFile3 := "images/3img.jpg"
  dirSlice3 := []string{rootPath0+"aFactory/3img/", rootPath0+"bFactory/3img/", rootPath1+"cFactory/3img/", rootPath1+"dFactory/3img/"}
  createDirStruc(dirSlice3)
  for _, dir := range dirSlice3 {
    go copyFile(srcFile3, dir, 0, 3269625)
  }

  srcFile4 := "images/4img.jpg"
  dirSlice4 := []string{rootPath0+"aFactory/4img/", rootPath0+"bFactory/4img/", rootPath1+"cFactory/4img/", rootPath1+"dFactory/4img/"}
  createDirStruc(dirSlice4)
  for _, dir := range dirSlice4 {
    go copyFile(srcFile4, dir, 0, 9434)
  }

  srcFile5 := "images/5img.jpg"
  dirSlice5 := []string{rootPath0+"aFactory/5img/", rootPath0+"bFactory/5img/", rootPath1+"cFactory/5img/", rootPath1+"dFactory/5img/"}
  createDirStruc(dirSlice5)
  for _, dir := range dirSlice5 {
    go copyFile(srcFile5, dir, 0, 3763)
  }
*/
  wg.Wait()
}
