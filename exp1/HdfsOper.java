import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.Scanner;

public class HdfsOper {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        Scanner sc = new Scanner(System.in);

        Path localSrc = new Path("./local.txt");
        Path hdfsDst = new Path("/user/hadoop/test/local.txt");

        // ==========1.上传文件，已存在选择覆盖/追加末尾==========
        System.out.println("\n=====1.上传文件测试=====");
        if (fs.exists(hdfsDst)) {
            System.out.println("HDFS目标文件已存在：1=覆盖，2=追加到末尾");
            int op = sc.nextInt();
            if(op == 1){
                fs.copyFromLocalFile(true,localSrc,hdfsDst);
                System.out.println("执行覆盖上传完成");
            }else if(op == 2){
                fs.copyFromLocalFile(false,localSrc,hdfsDst);
                System.out.println("执行追加上传完成");
            }
        } else {
            fs.copyFromLocalFile(localSrc, hdfsDst);
            System.out.println("文件不存在，直接上传完成");
        }

        // ==========2.下载文件，本地重名自动重命名==========
        System.out.println("\n=====2.下载文件测试=====");
        File localFile = new File("./local.txt");
        Path localOut;
        if(localFile.exists()){
            localOut = new Path("./local_1.txt");
            System.out.println("本地存在同名文件，自动重命名为 local_1.txt");
        }else{
            localOut = new Path("./local.txt");
        }
        fs.copyToLocalFile(false,hdfsDst,localOut,true);
        System.out.println("下载完成");

        // ==========3.HDFS文件内容输出控制台==========
        System.out.println("\n=====3.打印HDFS文件内容=====");
        FSDataInputStream in = fs.open(hdfsDst);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        br.close();

        // ==========4.获取文件元信息：权限、大小、时间、路径==========
        System.out.println("\n=====4.文件元信息=====");
        FileStatus status = fs.getFileStatus(hdfsDst);
        System.out.println("路径：" + status.getPath());
        System.out.println("大小(bytes)：" + status.getLen());
        System.out.println("权限：" + status.getPermission());
        System.out.println("修改时间：" + status.getModificationTime());

        // ==========5.递归输出目录下所有文件元信息==========
        System.out.println("\n=====5.递归遍历目录 /user/hadoop =====");
        listAllFileStatus(fs,new Path("/user/hadoop"));

        // ==========6.创建、删除文件，父目录不存在自动创建==========
        System.out.println("\n=====6.创建删除文件测试=====");
        Path fpath = new Path("/user/hadoop/dir1/file1.txt");
        if(!fs.exists(fpath.getParent())){
            fs.mkdirs(fpath.getParent());
            System.out.println("自动创建父目录");
        }
        fs.createNewFile(fpath);
        System.out.println("创建文件完成");
        fs.delete(fpath,false);
        System.out.println("删除文件完成");

        // ==========7.目录创建；仅空目录允许删除==========
        System.out.println("\n=====7.目录操作，仅空目录删除=====");
        Path dir = new Path("/user/hadoop/mydir/a/b");
        fs.mkdirs(dir);
        System.out.println("创建多级目录完成");
        FileStatus[] dirStatus = fs.listStatus(dir);
        if(dirStatus.length == 0){
            fs.delete(dir,false);
            System.out.println("目录为空，执行删除");
        }else{
            System.out.println("目录非空，不执行删除");
        }

        // ==========8.追加内容：选择追加开头 / 末尾==========
        System.out.println("\n=====8.追加内容：1=追加到开头，2=追加到末尾");
        int appendOpt = sc.nextInt();
        String addText = "\n【新增测试文本】";
        if(appendOpt == 1){
            // HDFS不支持头部追加：读全部，拼接，覆盖回写
            FSDataInputStream readIn = fs.open(hdfsDst);
            BufferedReader readBr = new BufferedReader(new InputStreamReader(readIn));
            StringBuilder sb = new StringBuilder();
            String tempLine;
            while((tempLine = readBr.readLine())!=null){
                sb.append(tempLine).append("\n");
            }
            readBr.close();
            readIn.close();
            String result = addText + sb.toString();
            fs.delete(hdfsDst,false);
            FSDataOutputStream outHead = fs.create(hdfsDst);
            outHead.write(result.getBytes());
            outHead.close();
            System.out.println("内容追加到文件【开头】完成");
        }else if(appendOpt == 2){
            FSDataOutputStream outTail = fs.append(hdfsDst);
            outTail.write(addText.getBytes());
            outTail.close();
            System.out.println("内容追加到文件【末尾】完成");
        }

        // ==========9.删除HDFS指定文件==========
        System.out.println("\n=====9.删除指定文件=====");
        fs.delete(hdfsDst,false);
        System.out.println("已删除目标测试文件");

        // ==========10.HDFS内部移动文件==========
        System.out.println("\n=====10.HDFS内部移动文件测试=====");
        Path srcMv = new Path("/user/hadoop/test/file.txt");
        Path dstMv = new Path("/user/hadoop/dir1/file.txt");
        fs.createNewFile(srcMv);
        fs.rename(srcMv,dstMv);
        System.out.println("文件移动完成");

        sc.close();
        fs.close();
    }

    //递归打印目录下所有文件状态信息，用于第5题
    public static void listAllFileStatus(FileSystem fs,Path path) throws IOException{
        FileStatus[] list = fs.listStatus(path);
        for(FileStatus s : list){
            System.out.println("path:"+s.getPath()+" size:"+s.getLen()+" perm:"+s.getPermission());
            if(s.isDirectory()){
                listAllFileStatus(fs,s.getPath());
            }
        }
    }
}

