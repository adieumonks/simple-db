package file

import (
	"fmt"
	"os"
	"path"
	"sync"
)

type FileManager struct {
	dbDirectory string
	blockSize   int32
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileManager(dirname string, blockSize int32) (*FileManager, error) {
	isNew := false
	if _, err := os.Stat(dirname); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to stat directory: %w", err)
		}
		isNew = true
	}

	if isNew {
		err := os.MkdirAll(dirname, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	return &FileManager{
		dbDirectory: dirname,
		blockSize:   blockSize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}, nil
}

func (fm *FileManager) Read(block *BlockID, page *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	_, err = f.Seek(int64(block.Number()*fm.blockSize), 0)
	if err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	_, err = f.Read(page.buffer)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}

func (fm *FileManager) Write(block *BlockID, page *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(block.Filename())
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	_, err = f.Seek(int64(block.Number()*fm.blockSize), 0)
	if err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	_, err = f.Write(page.buffer)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (fm *FileManager) Append(filename string) (*BlockID, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	newBlockNum, err := fm.Length(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get length: %w", err)
	}

	block := NewBlockID(filename, newBlockNum)
	b := make([]byte, fm.blockSize)

	f, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get file: %w", err)
	}

	_, err = f.Seek(int64(block.Number()*fm.blockSize), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek file: %w", err)
	}

	_, err = f.Write(b)
	if err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	return block, nil
}

func (fm *FileManager) Length(filename string) (int32, error) {
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to get file: %w", err)
	}

	length, err := f.Seek(0, 2)
	return int32(length) / fm.blockSize, err
}

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

func (fm *FileManager) BlockSize() int32 {
	return fm.blockSize
}

func (fm *FileManager) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
		return f, nil
	}

	f, err := os.OpenFile(path.Join(fm.dbDirectory, filename), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fm.openFiles[filename] = f

	return f, nil
}
