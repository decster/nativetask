/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FILEUTILS_H_
#define FILEUTILS_H_

#include "MapOutputCollector.h"

namespace Hadoop {


/**
 * K/V pair writer
 */
class MapOutputWriter {
protected:
  std::vector<SpillSegmentInfo> _spill_info;
public:
  SpillRangeInfo * get_spill_range_info(uint32_t start) {
    SpillSegmentInfo * segs = new SpillSegmentInfo[_spill_info.size()];
    for (size_t i = 0; i < _spill_info.size(); i++) {
      segs[i] = _spill_info[i];
    }
    std::string filename = "";
    return new SpillRangeInfo(start, (uint32_t)_spill_info.size(), filename, segs);
  }

  virtual void get_statistics(uint64_t & offset, uint64_t & realoffset) {
    if (_spill_info.size()>0) {
      offset = _spill_info[_spill_info.size()-1].end_position;
      realoffset = _spill_info[_spill_info.size()-1].real_end_position;
    } else{
      offset = 0;
      realoffset = 0;
    }
  }

  MapOutputWriter() {
  }

  virtual ~MapOutputWriter() {
  }

  virtual void start_partition() = 0;
  virtual void end_partition() = 0;
  virtual void write_key_part(const char * key, uint32_t key_len, uint32_t value_len) = 0;
  virtual void write_value_part(const char * value, uint32_t value_len) = 0;

  /**
   * util method to write k/v pair at once
   */
  void write(const char * key, uint32_t key_len, const char * value,
      uint32_t value_len) {
    write_key_part(key, key_len, value_len);
    write_value_part(value, value_len);
  }
};

/**
 * IFile Writer
 */
class IFileWriter : public MapOutputWriter {
private:
  int _fd;
  uint32_t checksum;
  uint64_t _offset;
  uint64_t _real_offset;
  AppendBuffer _append_buffer;
  KeyValueType _ktype;
  KeyValueType _vtype;
public:
  IFileWriter(int fd, char * buff, uint32_t size, KeyValueType ktype,
              KeyValueType vtype, CompressionType compression_type) :
      _fd(fd),
      _offset(0),
      _real_offset(0),
      _append_buffer(buff, size, fd, &checksum, &_real_offset, compression_type),
      _ktype(ktype),
      _vtype(vtype) {
  }

  virtual ~IFileWriter() {
  }

  void start_partition() {
    if (_ktype != TextType && _ktype != BytesType) {
      THROW_EXCEPTION(UnsupportException, "_kvtype not supported");
    }
    _spill_info.push_back(SpillSegmentInfo());
    checksum = checksum_init();
  }

  void end_partition() {
    _append_buffer.append(&IFileEndMarker, sizeof(IFileEndMarker));
    _offset += sizeof(IFileEndMarker);
    _append_buffer.flush();
    uint32_t chsum = checksum_val(checksum);
    chsum = bswap(chsum);
    WRITE(_fd, &chsum, sizeof(uint32_t));
    _real_offset += sizeof(uint32_t);
    SpillSegmentInfo * info = &(_spill_info[_spill_info.size()-1]);
    info->end_position = _offset;
    info->real_end_position = _real_offset;
  }

  void write_key_part(const char * key, uint32_t key_len, uint32_t value_len) {
    // append KeyLength ValueLength KeyBytesLength
    char * oldmark = _append_buffer.reserve_space(MaxVUInt32Length*3);
    char * mark = oldmark;
    if (_ktype == BytesType) {
      WriteVUInt32(mark, sizeof(uint32_t)+key_len);
      uint32_t  value_buff_len = sizeof(uint32_t)+value_len;
      WriteVUInt32(mark, value_buff_len);
      WriteUInt32(mark, key_len);
      mark += sizeof(uint32_t);
    }
    else { // TextType
      WriteVUInt32(mark, GetVUInt32Len(key_len)+key_len);
      uint32_t value_buff_len = GetVUInt32Len(value_len) + value_len;
      WriteVUInt32(mark, value_buff_len);
      WriteVUInt32(mark, key_len);
    }
    _append_buffer.reserve_space_used_to(mark);
    _offset += (mark-oldmark);
    // append Key
    if (key_len>0) {
      _append_buffer.append(key, key_len);
      _offset += key_len;
    }
  }

  void write_value_part(const char * value, uint32_t value_len) {
    // append ValueBytesLength
    if (_vtype == BytesType) {
      _append_buffer.append(bswap(value_len));
      _offset += sizeof(uint32_t);
    }
    else if (_vtype == TextType) { // TextTextType
      char * oldmark = _append_buffer.reserve_space(MaxVUInt32Length);
      char * mark = oldmark;
      WriteVUInt32(mark, value_len);
      _append_buffer.reserve_space_used_to(mark);
      _offset += (mark - oldmark);
    }
//    else { // unknown type
//      // nothing to do
//    }
    // append Value
    if (value_len>0) {
      _append_buffer.append(value, value_len);
      _offset += value_len;
    }
  }
};

/**
 * INTERMEDIATE file writer
 */
class IntermediateFileWriter : public MapOutputWriter {
private:
  int _fd;
  uint32_t checksum;
  uint64_t _offset;
  uint64_t _real_offset;
  AppendBuffer _append_buffer;
public:
  IntermediateFileWriter(int fd, char * buff, uint32_t size,
                         CompressionType compression_type) :
      _fd(fd),
      _offset(0),
      _real_offset(0),
      _append_buffer(buff, size, fd, &checksum, &_real_offset,
                     compression_type) {
  }

  virtual ~IntermediateFileWriter() {
  }

  void start_partition() {
    _spill_info.push_back(SpillSegmentInfo());
    checksum = checksum_init();
  }

  void end_partition() {
    _append_buffer.flush();
    uint32_t chsum = checksum_val(checksum);
    chsum = bswap(chsum);
    WRITE(_fd, &chsum, sizeof(uint32_t));
    _real_offset += sizeof(uint32_t);
    SpillSegmentInfo * info = &(_spill_info[_spill_info.size()-1]);
    info->end_position = _offset;
    info->real_end_position = _real_offset;
  }

  void write_key_part(const char * key, uint32_t key_len, uint32_t value_len) {
    _append_buffer.append(key_len);
    _offset += sizeof(uint32_t);
    _append_buffer.append(key, key_len);
    _offset += key_len;
  }

  void write_value_part(const char * value, uint32_t value_len) {
    _append_buffer.append(value_len);
    _offset += sizeof(uint32_t);
    _append_buffer.append(value, value_len);
    _offset += value_len;
  }

};


/**
 * IFileReader
 * Notice: INTERMEDIATE file reader is used for merge,
 * so this class is not used yet
 */
class IFileReader {
private:
  KeyValueType _ktype;
  KeyValueType _vtype;
  int _fd;
  uint64_t _offset;
  uint64_t _offset_end;
  uint64_t _real_offset; // not used yet
  int _current_segment_index;
  ReadBuffer _reader;
  uint32_t _current_value_buff_len;
  SpillRangeInfo * _spill_infos;
public:
  // TODO: unify interface with interfilereader
  IFileReader(int filefd, char * buffer, uint32_t size, KeyValueType ktype,
              KeyValueType vtype, SpillRangeInfo * spill_infos) :
      _ktype(ktype),
      _vtype(vtype),
      _fd(filefd),
      _offset(0),
      _offset_end(0),
      _real_offset(0),
      _current_segment_index(-1),
      _reader(buffer, size, _fd, &_offset, true),
      _current_value_buff_len(0),
      _spill_infos(spill_infos) {
  }
  ~IFileReader() {
  }

  /**
   * 0 if success
   * 1 if end
   */
  int next_partition() {
    _current_segment_index++;
    if (_current_segment_index < (int)(_spill_infos->length)) {
      _offset_end = _spill_infos->segments[_spill_infos->start + _current_segment_index].end_position
          - sizeof(IFileEndMarker);
      _reader.init_checksum();
      return 0;
    }
    else {
      return 1;
    }
  }

  /**
   * get next key
   * NULL if no more, then next_partition() need to be called
   * NOTICE: before value() is called, the return pointer value is
   *         guaranteed to be valid
   * TODO: handle big key
   */
  const char * next_key(uint32_t & key_len) {
    if (_offset < _offset_end) {
      //uint32_t kb_len = _reader.read_vuint();
      _reader.read_vuint();
      _current_value_buff_len = _reader.read_vuint();
      if (_ktype==TextType) {
        key_len = _reader.read_vuint();
      } else { // BytesBytesType
        key_len = _reader.read_uint_be();
      }
      return _reader.get(key_len);
    }
    else if (_offset == _offset_end) {
      char * eofm = _reader.get(sizeof(IFileEndMarker));
      if (*(uint16_t*)eofm != IFileEndMarker) {
        THROW_EXCEPTION(IOException, "IFile format error, eof marker error");
      }
      _reader.verify_checksum();
      return NULL;
    } else {
      THROW_EXCEPTION(IOException, "bad IFile stream");
    }
  }

  /**
   * length of current value part of IFile entry
   */
  const uint32_t value_buff_len() {
    return _current_value_buff_len;
  }

  /**
   * get current value
   * TODO: handle big value
   */
  const char * value(uint32_t & value_len) {
    switch (_vtype) {
    case TextType:
      value_len = _reader.read_vuint();
      break;
    case BytesType:
      value_len = _reader.read_uint_be();
      break;
    default:
      value_len = _current_value_buff_len;
    }
    return _reader.get(value_len);
  }
};

/**
 * reader for intermediate file
 */
class IntermediateFileReader {
private:
  FILE * _pfile;
  int _fd;
  ReadBuffer * _reader;
  uint64_t _offset;
  uint64_t _offset_end;
  uint64_t _real_offset; // not used yet
  char * _buff;
  uint32_t _buff_size;
  int _current_segment_index;
  SpillRangeInfo * _spill_infos;
  uint32_t _temp_value_len;

  // temp buff to hold big key/value
  char * _temp_buffer;
  size_t _temp_buffer_len;
public:
  IntermediateFileReader(SpillRangeInfo * spill_info, uint32_t buffsize) :
      _pfile(NULL),
      _fd(-1),
      _offset(0),
      _offset_end(0),
      _real_offset(0),
      _buff(NULL),
      _buff_size(buffsize),
      _current_segment_index(-1),
      _spill_infos(spill_info),
      _temp_buffer(NULL),
      _temp_buffer_len(0) {
    _pfile = fopen(spill_info->filepath.c_str(), "rb");
    if (_pfile) {
      _fd = fileno(_pfile);
      _buff_size = buffsize;
      _buff = new char[_buff_size];
      _reader = new ReadBuffer(_buff, buffsize, _fd, &_offset, true);
    }
  }

  ~IntermediateFileReader() {
    if (NULL != _temp_buffer) {
      delete [] _temp_buffer;
    }
    delete _reader;
    delete [] _buff;
    if (_pfile) {
      fclose(_pfile);
    }
  }

  /**
   * 0 if success
   * 1 if end
   */
  int next_partition() {
    _current_segment_index++;
    if (_current_segment_index < (int)(_spill_infos->length)) {
      _offset_end = _spill_infos->
          segments[_spill_infos->start + _current_segment_index].end_position;
      _reader->init_checksum();
      return 0;
    }
    else {
      return 1;
    }
  }

  /**
   * get next key
   * NULL if no more, then next_partition() need to be called
   * NOTICE: before value() is called, the return pointer value is
   *         guaranteed to be valid
   */
  const char * next_key(uint32_t & key_len) {
    char * ret = NULL;
    if (_offset < _offset_end) {
      key_len = _reader->read_uint32_le();
      // must read together
      uint32_t need = key_len + sizeof(uint32_t);
      ret = _reader->get(need);
      if (unlikely(NULL==ret)) {
        if (_temp_buffer_len<need) {
          if (NULL != _temp_buffer) {
            delete [] _temp_buffer;
          }
          _temp_buffer = new char[need];
          _temp_buffer_len = need;
        }
        _reader->read_to(_temp_buffer, need);
        ret = _temp_buffer;
      }
      _temp_value_len = *(uint32_t*)(ret+key_len);
    }
    else if (_offset == _offset_end) {
      _reader->verify_checksum();
    } else {
      THROW_EXCEPTION(IOException, "Bad IntermediateFile stream");
    }
    if (unlikely(NULL != _temp_buffer)) {
      if (ret != _temp_buffer) {
        delete [] _temp_buffer;
        _temp_buffer = NULL;
        _temp_buffer_len = 0;
      }
    }
    return ret;
  }

  uint32_t current_value_len() const {
    return _temp_value_len;
  }

  /**
   * get current value
   * TODO: handle big value without new char[] buffer
   */
  const char * value() {
    const char * ret = _reader->get(_temp_value_len);
    if (unlikely(NULL==ret)) {
      if (_temp_buffer_len<_temp_value_len) {
        if (NULL != _temp_buffer) {
          delete [] _temp_buffer;
        }
        _temp_buffer = new char[_temp_value_len];
        _temp_buffer_len = _temp_value_len;
      }
      _reader->read_to(_temp_buffer, _temp_value_len);
      ret = _temp_buffer;
    }
    return ret;
  }
};


} // namespace Hadoop


#endif /* FILEUTILS_H_ */
