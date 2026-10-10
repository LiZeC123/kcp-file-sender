package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	defaultPort      = 12080
	defaultFECData   = 0	// 由于丢包率过高, 因此不开启FEC
	defaultFECParity = 0
	defaultInterval  = 10
	defaultResend    = 2
	defaultNoDelay   = 1
	defaultNC        = 1
	defaultWindow    = 1024
	defaultMTU       = 1400

	maxNameLen   = 65535
	maxFileCount = 1000000

	// 客户端在收完所有文件并写盘后发送的确认字节
	ackDone = 'D'
	// 服务端等待客户端确认的超时
	serverAckTimeout = 120 * time.Second
	// 客户端在 recvFile 中的单次读超时, 避免服务端异常时永久阻塞
	clientReadTimeout = 120 * time.Second
)

// ---------------- 参数 ----------------

type KCPConfig struct {
	FECData   int
	FECParity int
	NoDelay   int
	Interval  int
	Resend    int
	NC        int
	Window    int
	MTU       int
}

func (c KCPConfig) apply(conn *kcp.UDPSession) {
	conn.SetNoDelay(c.NoDelay, c.Interval, c.Resend, c.NC)
	conn.SetWindowSize(c.Window, c.Window)
	conn.SetMtu(c.MTU)
}

type FileEntry struct {
	RelPath string // 相对路径, 斜杠分隔
	AbsPath string // 服务端本地绝对路径
	Size    int64
}

func main() {
	mode := flag.String("mode", "", "server 或 client")

	// server
	rootPath := flag.String("path", "", "server: 扫描的根路径")
	minSizeMiB := flag.Int64("minsize", 0, "server: 最小文件大小 (MiB)")
	port := flag.Int("port", defaultPort, "监听/连接端口")

	// client
	serverHost := flag.String("server", "", "client: 服务端地址 (IP 或域名)")
	outDir := flag.String("out", ".", "client: 保存目录")

	// KCP 参数
	fecData := flag.Int("fec-data", defaultFECData, "FEC 数据分片数")
	fecParity := flag.Int("fec-parity", defaultFECParity, "FEC 纠错分片数")
	interval := flag.Int("interval", defaultInterval, "KCP 内部 tick (ms)")
	resend := flag.Int("resend", defaultResend, "快速重传阈值")
	noDelay := flag.Int("nodelay", defaultNoDelay, "nodelay 模式")
	nc := flag.Int("nc", defaultNC, "拥塞控制 (1=关闭)")
	window := flag.Int("window", defaultWindow, "收发窗口大小 (分片数)")
	mtu := flag.Int("mtu", defaultMTU, "MTU")

	flag.Parse()

	cfg := KCPConfig{
		FECData:   *fecData,
		FECParity: *fecParity,
		NoDelay:   *noDelay,
		Interval:  *interval,
		Resend:    *resend,
		NC:        *nc,
		Window:    *window,
		MTU:       *mtu,
	}

	switch *mode {
	case "server":
		if *rootPath == "" {
			log.Fatal("server 模式需要 -path 参数")
		}
		runServer(*rootPath, *minSizeMiB, *port, cfg)
	case "client":
		if *serverHost == "" {
			log.Fatal("client 模式需要 -server 参数")
		}
		runClient(*serverHost, *port, *outDir, cfg)
	default:
		log.Fatal("需要 -mode server 或 -mode client")
	}
}

// ---------------- 服务端 ----------------

func runServer(rootPath string, minSizeMiB int64, port int, cfg KCPConfig) {
	minSize := minSizeMiB * 1024 * 1024
	log.Printf("扫描目录: %s, 最小大小: %d MiB", rootPath, minSizeMiB)

	files, err := scanFiles(rootPath, minSize)
	if err != nil {
		log.Fatal("扫描失败: ", err)
	}
	if len(files) == 0 {
		log.Fatal("未找到符合条件的文件")
	}

	log.Printf("找到 %d 个文件:", len(files))
	for i, f := range files {
		log.Printf("  [%d] %s (%.2f MiB)", i+1, f.RelPath, float64(f.Size)/1024/1024)
	}

	listenAddr := fmt.Sprintf(":%d", port)
	lis, err := kcp.ListenWithOptions(listenAddr, nil, cfg.FECData, cfg.FECParity)
	if err != nil {
		log.Fatal("KCP 监听失败: ", err)
	}
	defer lis.Close()
	log.Printf("KCP 服务端监听 %s (FEC %d/%d, window=%d, mtu=%d)",
		listenAddr, cfg.FECData, cfg.FECParity, cfg.Window, cfg.MTU)

	for {
		conn, err := lis.AcceptKCP()
		if err != nil {
			log.Printf("Accept 错误: %v", err)
			continue
		}
		log.Printf("客户端连接: %s", conn.RemoteAddr())
		handleClient(conn, files, cfg)
		log.Printf("客户端断开: %s", conn.RemoteAddr())
	}
}

func scanFiles(root string, minSize int64) ([]FileEntry, error) {
	var files []FileEntry
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if info.Size() < minSize {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, FileEntry{
			RelPath: filepath.ToSlash(rel),
			AbsPath: path,
			Size:    info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].RelPath < files[j].RelPath
	})
	return files, nil
}

func handleClient(conn *kcp.UDPSession, files []FileEntry, cfg KCPConfig) {
	cfg.apply(conn)

	// 1) 读握手字节 (触发 AcceptKCP 返回后必须读掉)
	hs := make([]byte, 1)
	if _, err := io.ReadFull(conn, hs); err != nil {
		log.Printf("握手失败: %v", err)
		conn.Close()
		return
	}
	log.Printf("握手完成, 发送文件列表 (%d 个)", len(files))

	// 2) 发送文件列表
	if err := sendFileList(conn, files); err != nil {
		log.Printf("发送列表失败: %v", err)
		conn.Close()
		return
	}

	// 3) 读取客户端请求
	reqHdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, reqHdr); err != nil {
		log.Printf("读取请求头失败: %v", err)
		conn.Close()
		return
	}
	count := binary.BigEndian.Uint32(reqHdr)
	if count == 0 || count > maxFileCount {
		log.Printf("非法请求数量: %d", count)
		conn.Close()
		return
	}
	seqs := make([]uint32, count)
	seqBuf := make([]byte, 4)
	for i := range seqs {
		if _, err := io.ReadFull(conn, seqBuf); err != nil {
			log.Printf("读取序号失败: %v", err)
			conn.Close()
			return
		}
		seqs[i] = binary.BigEndian.Uint32(seqBuf)
	}

	log.Printf("客户端请求 %d 个文件", count)

	// 4) 逐个发送
	startAll := time.Now()
	var totalSent int64
	for _, seq := range seqs {
		if int(seq) >= len(files) {
			log.Printf("序号 %d 越界, 跳过", seq)
			continue
		}
		f := files[seq]
		n, err := sendFile(conn, f)
		if err != nil {
			log.Printf("发送 %s 失败: %v", f.RelPath, err)
			conn.Close()
			return
		}
		totalSent += n
	}
	elapsed := time.Since(startAll)
	log.Printf("会话完成: %d 个文件, %d 字节, 用时 %v, 平均 %.2f MiB/s",
		len(seqs), totalSent, elapsed,
		float64(totalSent)/elapsed.Seconds()/1024/1024)

	// 5) 阻塞等待客户端确认。
	//    客户端只有在按精确长度收完所有文件并写盘之后才会发送 ackDone,
	//    所以收到它就等于确认数据已全部交付, 此时才可安全关闭连接。
	log.Printf("等待客户端确认...")
	ack := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(serverAckTimeout))
	if _, err := io.ReadFull(conn, ack); err != nil {
		log.Printf("等待客户端确认失败: %v", err)
	} else if ack[0] == ackDone {
		log.Printf("收到客户端完成确认, 数据已全部交付")
	} else {
		log.Printf("收到未知确认字节: %d", ack[0])
	}
	conn.SetReadDeadline(time.Time{})

	conn.Close()
}

func sendFileList(conn io.Writer, files []FileEntry) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(files)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	for _, f := range files {
		nameBytes := []byte(f.RelPath)
		if len(nameBytes) > maxNameLen {
			return fmt.Errorf("文件名过长: %s", f.RelPath)
		}
		entry := make([]byte, 2+len(nameBytes)+8)
		binary.BigEndian.PutUint16(entry[0:2], uint16(len(nameBytes)))
		copy(entry[2:2+len(nameBytes)], nameBytes)
		binary.BigEndian.PutUint64(entry[2+len(nameBytes):], uint64(f.Size))
		if _, err := conn.Write(entry); err != nil {
			return err
		}
	}
	return nil
}

func sendFile(conn io.Writer, f FileEntry) (int64, error) {
	file, err := os.Open(f.AbsPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// 文件头: [nameLen:2][name:N][size:8]
	nameBytes := []byte(f.RelPath)
	hdr := make([]byte, 2+len(nameBytes)+8)
	binary.BigEndian.PutUint16(hdr[0:2], uint16(len(nameBytes)))
	copy(hdr[2:2+len(nameBytes)], nameBytes)
	binary.BigEndian.PutUint64(hdr[2+len(nameBytes):], uint64(f.Size))
	if _, err := conn.Write(hdr); err != nil {
		return 0, err
	}

	start := time.Now()
	var sent int64
	buf := make([]byte, 64*1024)
	lastLog := start
	for {
		n, err := file.Read(buf)
		if n > 0 {
			if _, werr := conn.Write(buf[:n]); werr != nil {
				return sent, werr
			}
			sent += int64(n)
			if time.Since(lastLog) >= 2*time.Second {
				elapsed := time.Since(start).Seconds()
				rate := float64(sent) / elapsed / 1024 / 1024
				log.Printf("  ↑ %s: %.1f%% (%.2f MiB/s)", f.RelPath,
					float64(sent)*100/float64(f.Size), rate)
				lastLog = time.Now()
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return sent, err
		}
	}
	elapsed := time.Since(start)
	log.Printf("  ↑ %s 完成: %d 字节, %.2fs, 平均 %.2f MiB/s",
		f.RelPath, sent, elapsed.Seconds(),
		float64(sent)/elapsed.Seconds()/1024/1024)
	return sent, nil
}

// ---------------- 客户端 ----------------

func runClient(host string, port int, outDir string, cfg KCPConfig) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := kcp.DialWithOptions(addr, nil, cfg.FECData, cfg.FECParity)
	if err != nil {
		log.Fatal("连接失败: ", err)
	}
	defer conn.Close()
	cfg.apply(conn)

	// 发送握手字节, 触发服务端 AcceptKCP 返回
	if _, err := conn.Write([]byte{'H'}); err != nil {
		log.Fatal("握手失败: ", err)
	}
	log.Printf("已连接 %s (FEC %d/%d, window=%d, mtu=%d)",
		addr, cfg.FECData, cfg.FECParity, cfg.Window, cfg.MTU)

	// 接收文件列表
	files, err := recvFileList(conn)
	if err != nil {
		log.Fatal("接收列表失败: ", err)
	}
	if len(files) == 0 {
		log.Fatal("服务端没有可下载的文件")
	}

	fmt.Println()
	fmt.Printf("服务端文件列表 (共 %d 个):\n", len(files))
	for i, f := range files {
		fmt.Printf("  [%d] %-60s %10.2f MiB\n",
			i+1, f.RelPath, float64(f.Size)/1024/1024)
	}

	// 用户输入
	fmt.Print("\n输入序号 (空格分隔, q 退出): ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" || line == "q" || line == "quit" || line == "exit" {
		log.Println("已退出")
		return
	}

	// 解析序号
	parts := strings.Fields(line)
	var seqs []uint32
	seen := make(map[int]bool)
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			log.Fatalf("非法序号: %s", p)
		}
		if n < 1 || n > len(files) {
			log.Fatalf("序号越界: %d (有效范围 1-%d)", n, len(files))
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		seqs = append(seqs, uint32(n-1))
	}
	if len(seqs) == 0 {
		log.Fatal("未选择任何文件")
	}

	// 发送请求: [count:4] + [seq:4]*count
	req := make([]byte, 4+4*len(seqs))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(seqs)))
	for i, s := range seqs {
		binary.BigEndian.PutUint32(req[4+4*i:], s)
	}
	if _, err := conn.Write(req); err != nil {
		log.Fatal("发送请求失败: ", err)
	}

	fmt.Printf("\n开始下载 %d 个文件...\n\n", len(seqs))

	startAll := time.Now()
	var totalBytes int64
	for i := 0; i < len(seqs); i++ {
		localPath, size, err := recvFile(conn, outDir)
		if err != nil {
			fmt.Println()
			log.Fatalf("接收文件失败: %v", err)
		}
		fmt.Printf("  ↓ 已保存 %s (%d 字节, %.2f MiB)\n",
			localPath, size, float64(size)/1024/1024)
		totalBytes += size
	}

	// 所有文件已收完并写盘, 通知服务端可以安全关闭
	// 这是服务端确认数据全部交付的唯一依据
	if _, err := conn.Write([]byte{ackDone}); err != nil {
		log.Printf("发送完成确认失败: %v", err)
	}

	elapsed := time.Since(startAll)
	fmt.Printf("\n全部完成: %d 个文件, %d 字节 (%.2f MiB), 用时 %v, 平均 %.2f MiB/s\n",
		len(seqs), totalBytes, float64(totalBytes)/1024/1024,
		elapsed, float64(totalBytes)/elapsed.Seconds()/1024/1024)

	// 给 ackDone 一点时间通过 KCP 送达服务端, 然后退出
	time.Sleep(1 * time.Second)
}

func recvFileList(conn io.Reader) ([]FileEntry, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint32(hdr)
	if count > maxFileCount {
		return nil, fmt.Errorf("文件列表过大: %d", count)
	}
	files := make([]FileEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		nhdr := make([]byte, 2)
		if _, err := io.ReadFull(conn, nhdr); err != nil {
			return nil, err
		}
		nameLen := binary.BigEndian.Uint16(nhdr)
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(conn, nameBuf); err != nil {
			return nil, err
		}
		sizeBuf := make([]byte, 8)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint64(sizeBuf)
		if size > 1<<62 {
			return nil, fmt.Errorf("文件过大: %d", size)
		}
		files = append(files, FileEntry{
			RelPath: string(nameBuf),
			Size:    int64(size),
		})
	}
	return files, nil
}

func recvFile(conn *kcp.UDPSession, outDir string) (string, int64, error) {
	// 文件头: [nameLen:2][name:N][size:8]
	nhdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, nhdr); err != nil {
		return "", 0, err
	}
	nameLen := binary.BigEndian.Uint16(nhdr)
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return "", 0, err
	}
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return "", 0, err
	}
	remoteName := string(nameBuf)
	size := int64(binary.BigEndian.Uint64(sizeBuf))
	if size < 0 {
		return "", 0, fmt.Errorf("非法文件大小: %d", size)
	}

	// 平铺到 outDir, 处理重名
	baseName := filepath.Base(remoteName)
	localPath := uniquePath(outDir, baseName)

	out, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", 0, err
	}
	defer out.Close()

	start := time.Now()
	var received int64
	buf := make([]byte, 64*1024)
	lastUpdate := time.Now()

	for received < size {
		want := int64(len(buf))
		if size-received < want {
			want = size - received
		}
		// 单次读超时, 服务端异常时客户端不会永久阻塞
		conn.SetReadDeadline(time.Now().Add(clientReadTimeout))
		n, err := io.ReadFull(conn, buf[:want])
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return "", 0, werr
			}
			received += int64(n)
			if time.Since(lastUpdate) >= 100*time.Millisecond {
				printProgress(baseName, received, size, start)
				lastUpdate = time.Now()
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if received < size {
					return "", 0, fmt.Errorf("连接提前关闭: 收到 %d/%d 字节", received, size)
				}
				break
			}
			return "", 0, err
		}
	}
	conn.SetReadDeadline(time.Time{})
	printProgress(baseName, received, size, start)
	fmt.Println()
	return localPath, received, nil
}

// uniquePath 在 dir 下为 baseName 找一个不冲突的名字:
//
//	a.bin -> a_1.bin -> a_2.bin -> ...
func uniquePath(dir, baseName string) string {
	ext := filepath.Ext(baseName)
	stem := strings.TrimSuffix(baseName, ext)
	if stem == "" {
		stem = baseName
		ext = ""
	}
	candidate := filepath.Join(dir, baseName)
	if _, err := os.Stat(candidate); os.IsNotExist(err) {
		return candidate
	}
	for i := 1; ; i++ {
		candidate = filepath.Join(dir, fmt.Sprintf("%s_%d%s", stem, i, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}

func printProgress(name string, done, total int64, start time.Time) {
	const barWidth = 30
	frac := float64(done) / float64(total)
	if frac > 1 {
		frac = 1
	}
	if frac < 0 {
		frac = 0
	}

	finished := done >= total
	pct := frac * 100
	if finished {
		pct = 100.0
	}

	filled := int(frac * barWidth)
	if finished {
		filled = barWidth
	}
	bar := strings.Repeat("=", filled)
	if filled < barWidth {
		bar += ">" + strings.Repeat(" ", barWidth-filled-1)
	}

	elapsed := time.Since(start).Seconds()
	var rate float64
	if elapsed > 0 {
		rate = float64(done) / elapsed / 1024 / 1024
	}

	var eta string
	if finished {
		eta = "00:00"
	} else if rate > 0 {
		rem := float64(total-done) / (rate * 1024 * 1024)
		m := int(rem) / 60
		s := int(rem) % 60
		eta = fmt.Sprintf("%02d:%02d", m, s)
	} else {
		eta = "--:--"
	}

	displayName := name
	if len(displayName) > 40 {
		displayName = "..." + displayName[len(displayName)-37:]
	}

	fmt.Printf("\r%-40s [%s] %5.1f%%  %6.2f MiB/s  ETA %s",
		displayName, bar, pct, rate, eta)
}
