from enum import Enum
import DatabaseManager

class LockType(Enum):
	SHARED = 1
	EXCLUSIVE = 2

class LockManager:
	def __init__(self, site):
		self.grantedLocks = {}
		self.waitingLocks = {}
		self.site = site

	def initLockForKey(self, key):
		self.grantedLocks[key] = []
		self.waitingLocks[key] = []

	def resetLocks(self):
		for key in self.grantedLocks:
			self.grantedLocks[key] = []
			self.waitingLocks[key] = []

	def requestLock(self, transaction, key, lockType):
		TM = DatabaseManager.DatabaseManager.TM
		SM = DatabaseManager.DatabaseManager.SM
		DM = SM.sites[self.site]['site'].DM
		key_index = int(key[1:])

		if lockType == LockType.SHARED and key_index % 2 == 0 and DM.getLastCommitTime(key) < SM.sites[self.site]['startTime']:
			DatabaseManager.DatabaseManager.TM.rejectLock(transaction, self.site)
			return

		if len(self.grantedLocks[key]) == 0:
			self.grantedLocks[key].append({
				'type': lockType,
				'transaction': transaction
			})
			DatabaseManager.DatabaseManager.TM.grantLock(transaction, self.site, lockType)
		elif len(self.waitingLocks[key]) == 0 and lockType == LockType.SHARED and self.grantedLocks[key][0]['type'] == LockType.SHARED:
			self.grantedLocks[key].append({
				'type': lockType,
				'transaction': transaction
			})
			DatabaseManager.DatabaseManager.TM.grantLock(transaction, self.site, lockType)
		elif len(self.waitingLocks[key]) == 0 and lockType == LockType.EXCLUSIVE and len(self.grantedLocks[key]) == 1 and self.grantedLocks[key][0]['transaction'] == transaction:
			self.grantedLocks[key][0]['type'] = LockType.EXCLUSIVE
			DatabaseManager.DatabaseManager.TM.grantLock(transaction, self.site, lockType)
		else:
			self.waitingLocks[key].append({
				'type': lockType,
				'transaction': transaction
			})

	def releaseLock(self, transaction, key, committed):
		self.grantedLocks[key] = list(filter(lambda lock: lock['transaction'] != transaction, self.grantedLocks[key]))
		self.waitingLocks[key] = list(filter(lambda lock: lock['transaction'] != transaction, self.waitingLocks[key]))

		# If a transaction commits and any operation is pending on the site, it will proceed
		if committed:
			# Clear waiting locks. Allow pending operation for key to acquire locks. Append previously waiting to get lock
			tempWaitingLocks = self.waitingLocks[key]
			self.waitingLocks[key] = []
			SM = DatabaseManager.DatabaseManager.SM
			SM.doPendingOperationsForKey(self.site, key)
			self.waitingLocks[key].extend(tempWaitingLocks)

		# If a transaction aborts, and there were pending reads, there can only be write locks in the waiting queue. Normal execution can handle this scenario

		# No transactions waiting
		if len(self.waitingLocks[key]) == 0:
			return

		if len(self.grantedLocks[key]) == 0:
			seenSharedLock = False
			for lock in self.waitingLocks[key]:
				if not seenSharedLock and lock['type'] == LockType.EXCLUSIVE:
					self.grantedLocks[key].append(lock)
					DatabaseManager.DatabaseManager.TM.grantLock(lock['transaction'], self.site, lock['type'])
					break
				elif lock['type'] == LockType.EXCLUSIVE:
					break
				else:
					seenSharedLock = True
					self.grantedLocks[key].append(lock)
					DatabaseManager.DatabaseManager.TM.grantLock(lock['transaction'], self.site, lock['type'])
		elif len(self.grantedLocks[key]) == 1 and self.grantedLocks[key][0]['type'] == LockType.SHARED and self.waitingLocks[key][0]['transaction'] == self.grantedLocks[key][0]['transaction']:
			self.grantedLocks[key][0]['type'] = self.waitingLocks[key][0]['type']
			DatabaseManager.DatabaseManager.TM.grantLock(self.waitingLocks[key][0]['transaction'], self.site, self.waitingLocks[key][0]['type'])

		self.waitingLocks[key] = self.waitingLocks[key][len(self.grantedLocks[key]):]
