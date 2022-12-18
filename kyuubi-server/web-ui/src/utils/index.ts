function secondTransfer(val: number) {
  let theTime = val
  let middle = 0
  let hour = 0

  if (theTime >= 60) {
    middle = Math.floor(theTime / 60)
    theTime = Math.floor(theTime % 60)
    if (middle >= 60) {
      hour = Math.floor(middle / 60)
      middle = Math.floor(middle % 60)
    }
  }
  let result = String(Math.floor(theTime))
  if (middle > 0) {
    result = Math.floor(middle) + ':' + result
  }
  if (hour > 0) {
    result = Math.floor(hour) + ':' + result
  }
  return result
}

export { secondTransfer }
