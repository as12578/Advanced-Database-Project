class DataManager:
	def __init__(self, site):
		self.data = {}
		self.site = site
		self.committed = {}

	def initValue(self, key, value):
		self.data[key] = [{
			'transaction': '',
			'value': value,
			'committedTime': 0
		}]
		self.committed[key] = 0

	def setValue(self, transaction, key, value):
		lastCommittedIndex = self.committed[key]
		if len(self.data[key]) > lastCommittedIndex + 1:
			self.data[key][lastCommittedIndex + 1]['value'] = value
		else:
			self.data[key].append({
				'transaction': transaction,
				'value': value,
				'committedTime': -1
			})

	def dump(self):
		dumpOut = 'Site %s - ' % (self.site)
		for key in self.data.keys():
			lastCommittedIndex = self.committed[key]
			dumpOut += '%s: %s ' % (key, self.data[key][lastCommittedIndex]['value'])
		print(dumpOut.strip())

	def dumpKey(self, key):
		lastCommittedIndex = self.committed[key]
		dumpOut = 'Site %s - ' % (self.site)
		dumpOut += '%s: %s ' % (key, self.data[key][lastCommittedIndex]['value'])
		print(dumpOut.strip())

	def getValue(self, transaction, key):
		for i in reversed(range(self.committed[key], len(self.data[key]))):
			if self.data[key][i]['transaction'] == transaction:
				return self.data[key][i]

		return self.data[key][self.committed[key]]

	def readVersionAtTime(self, transaction, key, time):
		value = self.data[key][0]['value']
		for valueObj in self.data[key]:
			if valueObj['committedTime'] == -1 or valueObj['committedTime'] > time:
				break
			value = valueObj['value']

		return value

	def commitTransactionKey(self, transaction, key, commitTime):
		lastCommittedIndex = self.committed[key]
		if len(self.data[key]) > lastCommittedIndex + 1 and self.data[key][lastCommittedIndex + 1]['transaction'] == transaction:
			self.committed[key] = len(self.data[key]) - 1
			self.data[key][lastCommittedIndex + 1]['committedTime'] = commitTime

	def abortTransactionKey(self, transaction, key, commitTime):
		lastCommittedIndex = self.committed[key]
		self.data[key] = self.data[key][:lastCommittedIndex + 1]