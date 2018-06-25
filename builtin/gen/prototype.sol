// Copyright (c) 2018 The VeChainThor developers
 
// Distributed under the GNU Lesser General Public License v3.0 software license, see the accompanying
// file LICENSE or <https://www.gnu.org/licenses/lgpl-3.0.html>

pragma solidity 0.4.24;

/// @title Prototype extends account.
contract Prototype {

    function master(address self) public view returns(address){
        return PrototypeNative(this).native_master(self);
    }

    function setMaster(address self, address newMaster) public {
        require(self == msg.sender || PrototypeNative(this).native_master(self) == msg.sender, "builtin: self or master required");
        PrototypeNative(this).native_setMaster(self, newMaster);
    }

    function balance(address self, uint blockNumber) public view returns(uint256){
        return  PrototypeNative(this).native_balanceAtBlock(self, uint32(blockNumber));
    }

    function energy(address self, uint blockNumber) public view returns(uint256){
        return  PrototypeNative(this).native_energyAtBlock(self, uint32(blockNumber));
    }

    function hasCode(address self) public view returns(bool){
        return PrototypeNative(this).native_hasCode(self);
    }

    function storageFor(address self, bytes32 key) public view returns(bytes32){
        return PrototypeNative(this).native_storageFor(self, key);
    }

    function creditPlan(address self) public view returns(uint256 credit, uint256 recoveryRate){
        return PrototypeNative(this).native_creditPlan(self);
    }

    function setCreditPlan(address self, uint256 credit, uint256 recoveryRate) public{
        require(self == msg.sender || PrototypeNative(this).native_master(self) == msg.sender, "builtin: self or master required");
        PrototypeNative(this).native_setCreditPlan(self, credit, recoveryRate);
    }

    function isUser(address self, address user) public view returns(bool){
        return PrototypeNative(this).native_isUser(self, user);
    }

    function userCredit(address self, address user) public view returns(uint256){
        return PrototypeNative(this).native_userCredit(self, user);
    }

    function addUser(address self, address user) public{
        require(self == msg.sender || PrototypeNative(this).native_master(self) == msg.sender, "builtin: self or master required");
        require(PrototypeNative(this).native_addUser(self, user), "builtin: already added");        
    }

    function removeUser(address self, address user) public{
        require(self == msg.sender || PrototypeNative(this).native_master(self) == msg.sender, "builtin: self or master required");
        require(PrototypeNative(this).native_removeUser(self, user), "builtin: not a user");        
    }

    function sponsor(address self) public{
        require(PrototypeNative(this).native_sponsor(self, msg.sender), "builtin: already sponsored");
    }

    function unsponsor(address self) public {
        require(PrototypeNative(this).native_unsponsor(self, msg.sender), "builtin: not sponsored");        
    }

    function isSponsor(address self, address sponsorAddress) public view returns(bool) {
        return PrototypeNative(this).native_isSponsor(self, sponsorAddress);
    }

    function selectSponsor(address self, address sponsorAddress) public {
        require(self == msg.sender || PrototypeNative(this).native_master(self) == msg.sender, "builtin: self or master required");
        require(PrototypeNative(this).native_selectSponsor(self, sponsorAddress), "builtin: not a sponsor");
    }
    
    function currentSponsor(address self) public view returns(address){
        return PrototypeNative(this).native_currentSponsor(self);
    }
}

contract PrototypeNative {
    function native_master(address self) public view returns(address);
    function native_setMaster(address self, address newMaster) public;

    function native_balanceAtBlock(address self, uint32 blockNumber) public view returns(uint256);
    function native_energyAtBlock(address self, uint32 blockNumber) public view returns(uint256);
    function native_hasCode(address self) public view returns(bool);
    function native_storageFor(address self, bytes32 key) public view returns(bytes32);

    function native_creditPlan(address self) public view returns(uint256, uint256);
    function native_setCreditPlan(address self, uint256 credit, uint256 recoveryRate) public;

    function native_isUser(address self, address user) public view returns(bool);
    function native_userCredit(address self, address user) public view returns(uint256);
    function native_addUser(address self, address user) public returns(bool);
    function native_removeUser(address self, address user) public returns(bool);

    function native_sponsor(address self, address sponsor) public returns(bool);
    function native_unsponsor(address self, address sponsor) public returns(bool);
    function native_isSponsor(address self, address sponsor) public view returns(bool);
    function native_selectSponsor(address self, address sponsor) public returns(bool);
    function native_currentSponsor(address self) public view returns(address);
}

library PrototypeEvent {
    event $Master(address indexed newMaster);
    event $CreditPlan(uint256 credit, uint256 recoveryRate);
    event $User(address indexed user, bytes32 action);
    event $Sponsor(address indexed sponsor, bytes32 action);
}
