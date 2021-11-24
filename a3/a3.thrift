service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void put_backup(1: string key, 2: string value);
  void copy_to_backup(1: map<string, string> data);

}
